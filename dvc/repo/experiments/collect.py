import itertools
import os
from dataclasses import fields
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from funcy import first
from scmrepo.exceptions import SCMError as InnerSCMError

from dvc.log import logger
from dvc.scm import Git, SCMError, iter_revs

from .exceptions import InvalidExpRefError
from .refs import EXEC_BRANCH, ExpRefInfo
from .serialize import ExpRange, ExpState, SerializableError, SerializableExp
from .utils import describe

if TYPE_CHECKING:
    from dvc.repo import Repo

    from .cache import ExpCache

logger = logger.getChild(__name__)


def collect_rev(
    repo: "Repo",
    rev: str,
    param_deps: bool = False,
    force: bool = False,
    cache: Optional["ExpCache"] = None,
    **kwargs,
) -> ExpState:
    """Collect experiment state for the given revision.

    Exp will be loaded from cache when available unless rev is 'workspace' or
    force is set.
    """
    from dvc.fs import LocalFileSystem

    cache = cache or repo.experiments.cache
    assert cache
    # TODO: support filtering serialized exp when param_deps is set
    if rev != "workspace" and not (force or param_deps):
        cached_exp = cache.get(rev)
        if cached_exp:
            if isinstance(cached_exp, SerializableError):
                return ExpState(rev=rev, error=cached_exp)
            return ExpState(rev=rev, data=cached_exp)
    if rev == "workspace" and isinstance(repo.fs, LocalFileSystem):
        orig_cwd: Optional[str] = os.getcwd()
        os.chdir(repo.root_dir)
    else:
        orig_cwd = None
    try:
        data = _collect_rev(
            repo,
            rev,
            param_deps=param_deps,
            force=force,
            **kwargs,
        )
        if not (rev == "workspace" or param_deps or data.contains_error):
            cache.put(data, force=True)
        return ExpState(rev=rev, data=data)
    except Exception as exc:  # noqa: BLE001
        logger.debug("", exc_info=True)
        error = SerializableError(str(exc), type(exc).__name__)
        return ExpState(rev=rev, error=error)
    finally:
        if orig_cwd:
            os.chdir(orig_cwd)


def _collect_rev(
    repo: "Repo",
    rev: str,
    param_deps: bool = False,
    **kwargs,
) -> SerializableExp:
    with repo.switch(rev) as rev:
        if rev == "workspace":
            timestamp: Optional[datetime] = None
        else:
            commit = repo.scm.resolve_commit(rev)
            timestamp = datetime.fromtimestamp(commit.commit_time)

        return SerializableExp.from_repo(
            repo,
            rev=rev,
            param_deps=param_deps,
            timestamp=timestamp,
        )


def collect_branch(
    repo: "Repo",
    rev: str,
    end_rev: Optional[str] = None,
    **kwargs,
) -> Iterator["ExpState"]:
    """Iterate over exp states in a Git branch.

    Git branch will be traversed in reverse, starting from rev.

    Args:
        rev: Branch tip (head).
        end_rev: If specified, traversal will stop when end_rev is reached
            (exclusive, end_rev will not be collected).
    """
    try:
        for branch_rev in repo.scm.branch_revs(rev, end_rev):
            yield collect_rev(repo, branch_rev, **kwargs)
    except (SCMError, InnerSCMError):
        pass


def collect_exec_branch(
    repo: "Repo",
    baseline_rev: str,
    **kwargs,
) -> Iterator["ExpState"]:
    """Iterate over active experiment branch for the current executor."""
    last_rev = repo.scm.get_ref(EXEC_BRANCH) or repo.scm.get_rev()
    last_rev = repo.scm.get_rev()
    yield collect_rev(repo, "workspace", **kwargs)
    if last_rev != baseline_rev:
        yield from collect_branch(repo, last_rev, baseline_rev, **kwargs)


def collect_queued(
    repo: "Repo",
    baseline_revs: Collection[str],
    **kwargs,
) -> Dict[str, List["ExpRange"]]:
    """Collect queued experiments derived from the specified revisions.

    Args:
        repo: Repo.
        baseline_revs: Resolved baseline Git SHAs.

    Returns:
        Dict mapping baseline revision to list of queued experiments.
    """
    if not baseline_revs:
        return {}
    queued_data = {}
    for rev, ranges in repo.experiments.celery_queue.collect_queued_data(
        baseline_revs, **kwargs
    ).items():
        for exp_range in ranges:
            for exp_state in exp_range.revs:
                if exp_state.data:
                    attrs = [f.name for f in fields(SerializableExp)]
                    exp_state.data = SerializableExp(
                        **{
                            attr: getattr(exp_state.data, attr)
                            for attr in attrs
                            if attr != "metrics"
                        }
                    )
        queued_data[rev] = ranges
    return queued_data


def collect_active(
    repo: "Repo",
    baseline_revs: Collection[str],
    **kwargs,
) -> Dict[str, List["ExpRange"]]:
    """Collect active (running) experiments derived from the specified revisions.

    Args:
        repo: Repo.
        baseline_revs: Resolved baseline Git SHAs.

    Returns:
        Dict mapping baseline revision to list of active experiments.
    """
    if not baseline_revs:
        return {}
    result: Dict[str, List["ExpRange"]] = {}
    exps = repo.experiments
    for queue in (exps.workspace_queue, exps.tempdir_queue, exps.celery_queue):
        for baseline, active_exps in queue.collect_active_data(
            baseline_revs, **kwargs
        ).items():
            if baseline in result:
                result[baseline].extend(active_exps)
            else:
                result[baseline] = list(active_exps)
    return result


def collect_failed(
    repo: "Repo",
    baseline_revs: Collection[str],
    **kwargs,
) -> Dict[str, List["ExpRange"]]:
    """Collect failed experiments derived from the specified revisions.

    Args:
        repo: Repo.
        baseline_revs: Resolved baseline Git SHAs.

    Returns:
        Dict mapping baseline revision to list of active experiments.
    """
    if not baseline_revs:
        return {}
    return repo.experiments.celery_queue.collect_failed_data(baseline_revs, **kwargs)


def collect_successful(
    repo: "Repo",
    baseline_revs: Collection[str],
    **kwargs,
) -> Dict[str, List["ExpRange"]]:
    """Collect successful experiments derived from the specified revisions.

    Args:
        repo: Repo.
        baseline_revs: Resolved baseline Git SHAs.

    Returns:
        Dict mapping baseline revision to successful experiments.
    """
    result: Dict[str, List["ExpRange"]] = {}
    for baseline_rev in baseline_revs:
        result[baseline_rev] = list(_collect_baseline(repo, baseline_rev, **kwargs))
    return result


def _collect_baseline(
    repo: "Repo",
    baseline_rev: str,
    **kwargs,
) -> Iterator["ExpRange"]:
    """Iterate over experiments derived from a baseline revision.

    Args:
        repo: Repo.
        baseline_revs: Resolved baseline Git SHAs.

    Yields:
        Tuple of (timestamp, exp_range).
    """
    ref_info = ExpRefInfo(baseline_sha=baseline_rev)
    refs: Optional[Iterable[str]] = kwargs.get("refs")
    if refs:
        ref_it = (ref for ref in iter(refs) if ref.startswith(str(ref_info)))
    else:
        ref_it = repo.scm.iter_refs(base=str(ref_info))
    for ref in ref_it:
        try:
            ref_info = ExpRefInfo.from_ref(ref)
            exp_rev = repo.scm.get_ref(ref)
            if not exp_rev:
                continue
        except (InvalidExpRefError, SCMError, InnerSCMError):
            continue
        exps = list(collect_branch(repo, exp_rev, baseline_rev, **kwargs))
        if exps:
            exps[0].name = ref_info.name
            yield ExpRange(exps, name=ref_info.name)


def collect(
    repo: "Repo",
    revs: Union[List[str], str, None] = None,
    all_branches: bool = False,
    all_tags: bool = False,
    all_commits: bool = False,
    num: int = 1,
    hide_queued: bool = False,
    hide_failed: bool = False,
    sha_only: bool = False,
    **kwargs,
) -> List["ExpState"]:
    """Collect baseline revisions and derived experiments."""
    assert isinstance(repo.scm, Git)
    if repo.scm.no_commits:
        return []
    if not any([revs, all_branches, all_tags, all_commits]):
        revs = ["HEAD"]
    if isinstance(revs, str):
        revs = [revs]
    cached_refs = list(repo.scm.iter_refs())
    baseline_revs = list(
        iter_revs(
            repo.scm,
            revs=revs,
            num=num,
            all_branches=all_branches,
            all_tags=all_tags,
            all_commits=all_commits,
        )
    )
    if sha_only:
        baseline_names: Dict[str, Optional[str]] = {}
    else:
        baseline_names = describe(
            repo.scm, baseline_revs, refs=cached_refs, logger=logger
        )

    workspace_data = collect_rev(repo, "workspace", **kwargs)
    result: List["ExpState"] = [workspace_data]
    queued = collect_queued(repo, baseline_revs, **kwargs) if not hide_queued else {}
    active = collect_active(repo, baseline_revs, **kwargs)
    failed = collect_failed(repo, baseline_revs, **kwargs) if not hide_failed else {}
    successful = collect_successful(repo, baseline_revs, **kwargs)

    for baseline_rev in baseline_revs:
        baseline_data = collect_rev(repo, baseline_rev)
        experiments = list(
            itertools.chain.from_iterable(
                _sorted_ranges(collected.get(baseline_rev, []))
                for collected in (active, successful, queued, failed)
            )
        )
        result.append(
            ExpState(
                rev=baseline_rev,
                name=baseline_names.get(baseline_rev),
                data=baseline_data.data,
                error=baseline_data.error,
                experiments=experiments if experiments else None,
            )
        )
    return result


def _sorted_ranges(exp_ranges: Iterable["ExpRange"]) -> List["ExpRange"]:
    """Return list of ExpRange sorted by (timestamp, rev)."""

    def _head_timestamp(exp_range: "ExpRange") -> Tuple[datetime, str]:
        head_exp = first(exp_range.revs)
        if head_exp and head_exp.data and head_exp.data.timestamp:
            return head_exp.data.timestamp, head_exp.rev

        return datetime.fromtimestamp(0), ""

    return sorted(exp_ranges, key=_head_timestamp, reverse=True)
