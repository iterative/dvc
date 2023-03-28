import logging
from collections import OrderedDict, defaultdict
from datetime import datetime
from enum import Enum
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from scmrepo.exceptions import SCMError as InnerScmError

from dvc.exceptions import DvcException
from dvc.scm import Git, SCMError, iter_revs, resolve_rev

from .refs import ExpRefInfo
from .serialize import SerializableError, SerializableExp

if TYPE_CHECKING:
    from scmrepo.git.objects import GitCommit

    from dvc.repo import Repo

logger = logging.getLogger(__name__)


class ExpStatus(Enum):
    Success = 0
    Queued = 1
    Running = 2
    Failed = 3


class _CachedError(DvcException):
    def __init__(self, msg, typ, *args):
        super().__init__(msg, *args)
        self.typ = typ


def _is_scm_error(collected_exp: Dict[str, Any]) -> bool:
    if "error" in collected_exp and (
        isinstance(collected_exp["error"], (_CachedError, SCMError, InnerScmError))
    ):
        return True
    return False


def _format_exp(exp: SerializableExp) -> Dict[str, Any]:
    # SerializableExp always includes error but we need to strip it from show
    # output when it is false-y to maintain compatibility w/tools that consume
    # json output and assume that "error" key presence means there was an error
    exp_dict = exp.dumpd()
    if "error" in exp_dict and not exp_dict["error"]:
        del exp_dict["error"]
    return {"data": exp_dict}


def _format_error(error: SerializableError):
    msg = error.msg or "None"
    return {"data": {}, "error": _CachedError(msg, error.type)}


def collect_experiment_commit(
    repo: "Repo",
    exp_rev: str,
    status: ExpStatus = ExpStatus.Success,
    param_deps: bool = False,
    force: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    cache = repo.experiments.cache
    # TODO: support filtering serialized exp when param_deps is set
    if exp_rev != "workspace" and not (force or param_deps):
        cached_exp = cache.get(exp_rev)
        if cached_exp:
            if status == ExpStatus.Running or (
                isinstance(cached_exp, SerializableExp)
                and cached_exp.status == ExpStatus.Running.name
            ):
                # expire cached queued exp entry once we start running it
                cache.delete(exp_rev)
            elif isinstance(cached_exp, SerializableError):
                return _format_error(cached_exp)
            else:
                return _format_exp(cached_exp)
    try:
        exp = _collect_from_repo(
            repo,
            exp_rev,
            status=status,
            param_deps=param_deps,
            force=force,
            **kwargs,
        )
        if not (
            exp_rev == "workspace"
            or status == ExpStatus.Running
            or param_deps
            or exp.contains_error
        ):
            cache.put(exp, force=True)
        return _format_exp(exp)
    except Exception as exc:  # noqa: BLE001, pylint: disable=broad-except
        logger.debug("", exc_info=True)
        error = SerializableError(str(exc), type(exc).__name__)
        return _format_error(error)


def _collect_from_repo(
    repo: "Repo",
    exp_rev: str,
    status: ExpStatus = ExpStatus.Success,
    running: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> "SerializableExp":
    running = running or {}
    with repo.switch(exp_rev) as rev:
        if rev == "workspace":
            timestamp: Optional[datetime] = None
        else:
            commit = repo.scm.resolve_commit(rev)
            timestamp = datetime.fromtimestamp(commit.commit_time)

        if status == ExpStatus.Running:
            executor: Optional[str] = running.get(exp_rev, {}).get("location", None)
        else:
            executor = None

        if status == ExpStatus.Failed:
            error: Optional["SerializableError"] = SerializableError(
                "Experiment run failed."
            )
        else:
            error = None

        return SerializableExp.from_repo(
            repo,
            rev=exp_rev,
            timestamp=timestamp,
            status=status.name,
            executor=executor,
            error=error,
        )


def _collect_complete_experiment(
    repo: "Repo",
    baseline: str,
    exp_rev: str,
    running: Dict[str, Any],
    revs: List[str],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    results: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()

    checkpoint = len(revs) > 1
    prev = ""

    for rev in revs:
        status = ExpStatus.Running if rev in running else ExpStatus.Success
        collected_exp = collect_experiment_commit(
            repo,
            rev,
            status=status,
            running=running,
            **kwargs,
        )
        if _is_scm_error(collected_exp):
            return {}
        if checkpoint:
            exp = {"checkpoint_tip": exp_rev}
            if prev:
                results[prev]["data"][  # type: ignore[unreachable]
                    "checkpoint_parent"
                ] = rev
            if rev in results:
                results[rev]["data"].update(exp)
                results.move_to_end(rev)
            else:
                exp.update(collected_exp["data"])
        else:
            exp = collected_exp["data"]
        if rev not in results:
            results[rev] = {"data": exp}
        prev = rev
    if checkpoint and prev:
        results[prev]["data"]["checkpoint_parent"] = baseline
    return results


def _collect_branch(
    repo: "Repo",
    baseline: str,
    running: Dict[str, Any],
    refs: Optional[Iterable[str]] = None,
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = OrderedDict()
    status = ExpStatus.Running if baseline in running else ExpStatus.Success
    results["baseline"] = collect_experiment_commit(
        repo,
        baseline,
        status=status,
        running=running,
        **kwargs,
    )
    if baseline == "workspace" or _is_scm_error(results["baseline"]):
        return results

    ref_info = ExpRefInfo(baseline_sha=baseline)
    commits: List[Tuple[str, "GitCommit", str, List[str]]] = []

    assert isinstance(repo.scm, Git)
    if refs:
        ref_it = (ref for ref in iter(refs) if ref.startswith(str(ref_info)))
    else:
        ref_it = repo.scm.iter_refs(base=str(ref_info))
    for ref in ref_it:
        try:
            commit = repo.scm.resolve_commit(ref)
            exp_rev = resolve_rev(repo.scm, ref)
            revs = list(repo.scm.branch_revs(exp_rev, baseline))
        except (SCMError, InnerScmError):
            continue
        commits.append((ref, commit, exp_rev, revs))

    for exp_ref, _, exp_rev, revs in sorted(
        commits, key=lambda x: x[1].commit_time, reverse=True
    ):
        ref_info = ExpRefInfo.from_ref(exp_ref)
        assert ref_info.baseline_sha == baseline
        collected_exp = _collect_complete_experiment(
            repo,
            baseline=baseline,
            exp_rev=exp_rev,
            running=running,
            revs=revs,
            **kwargs,
        )
        if _is_scm_error(collected_exp):
            continue
        results.update(collected_exp)
    return results


def get_branch_names(
    scm: "Git", baseline_set: Iterable[str], refs: Optional[Iterable[str]] = None
) -> Dict[str, Optional[str]]:
    names: Dict[str, Optional[str]] = {}
    bases = [
        f"refs/exps/{baseline[:2]}/{baseline[2:]}/" for baseline in baseline_set
    ] + ["refs/heads/", "refs/tags/"]
    ref_it = iter(refs) if refs else scm.iter_refs()
    for ref in ref_it:
        for base in bases:
            if ref.startswith(base):
                try:
                    rev = scm.get_ref(ref)
                    names[rev] = ref[len(base) :]
                except KeyError:
                    logger.debug("unresolved ref %s", ref)
                break
    logger.debug("found refs for revs: %s", names)
    return names


def update_names(  # noqa: C901
    repo: "Repo",
    branch_names: Dict[str, Optional[str]],
    result: Dict[str, Dict[str, Any]],
):
    rev_set = set()
    for baseline in result:
        for rev in result[baseline]:
            if rev == "baseline":
                rev = baseline
            if rev != "workspace":
                rev_set.add(rev)

    if rev_set:
        rev_set.difference_update(branch_names.keys())

    exact_name = repo.experiments.get_exact_name(rev_set)

    for baseline, baseline_results in result.items():
        name_set: Set[str] = set()
        for rev, rev_result in baseline_results.items():
            name: Optional[str] = None
            if rev == "baseline":
                rev = baseline
                if rev == "workspace":
                    continue
            name = branch_names.get(rev, None) or exact_name[rev]
            if name and name not in name_set:
                name_set.add(name)
                rev_result["data"]["name"] = name


def _collect_active_experiment(
    repo: "Repo",
    found_revs: Dict[str, List[str]],
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict] = defaultdict(OrderedDict)
    for entry in chain(
        repo.experiments.tempdir_queue.iter_active(),
        repo.experiments.celery_queue.iter_active(),
    ):
        stash_rev = entry.stash_rev
        if entry.baseline_rev in found_revs and (
            stash_rev not in running or not running[stash_rev].get("last")
        ):
            collected_exp = collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Running,
                running=running,
                **kwargs,
            )
            if _is_scm_error(collected_exp):
                continue
            result[entry.baseline_rev][stash_rev] = collected_exp
    return result


def _collect_queued_experiment(
    repo: "Repo",
    found_revs: Dict[str, List[str]],
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict] = defaultdict(OrderedDict)
    for entry in repo.experiments.celery_queue.iter_queued():
        stash_rev = entry.stash_rev
        if entry.baseline_rev in found_revs:
            collected_exp = collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Queued,
                running=running,
                **kwargs,
            )
            if _is_scm_error(collected_exp):
                continue
            result[entry.baseline_rev][stash_rev] = collected_exp
    return result


def _collect_failed_experiment(
    repo: "Repo",
    found_revs: Dict[str, List[str]],
    running: Dict[str, Any],
    **kwargs,
) -> Dict[str, Dict[str, Any]]:
    result: Dict[str, Dict] = defaultdict(OrderedDict)
    for queue_done_result in repo.experiments.celery_queue.iter_failed():
        entry = queue_done_result.entry
        stash_rev = entry.stash_rev
        if entry.baseline_rev in found_revs:
            collected_exp = collect_experiment_commit(
                repo,
                stash_rev,
                status=ExpStatus.Failed,
                running=running,
                **kwargs,
            )
            if _is_scm_error(collected_exp):
                continue
            result[entry.baseline_rev][stash_rev] = collected_exp
    return result


def update_new(
    to_dict: Dict[str, Dict[str, Any]], from_dict: Dict[str, Dict[str, Any]]
):
    for baseline, experiments in from_dict.items():
        for rev, experiment in experiments.items():
            to_dict[baseline][rev] = to_dict[baseline].get(rev, experiment)


def move_properties_to_head(result: Dict[str, Dict[str, Dict[str, Any]]]):
    for _, baseline_results in result.items():
        checkpoint: bool = False
        head: Dict[str, Any] = {}
        for rev, rev_data in baseline_results.items():
            if (
                "data" not in rev_data
                or rev_data["data"].get("checkpoint_tip", None) is None
            ):
                checkpoint = False
                head = {}
                continue

            rev_result: Dict[str, Any] = rev_data["data"]
            if (
                checkpoint is True
                and rev_result["checkpoint_tip"] == head["checkpoint_tip"]
            ):
                if "name" in rev_result and "name" not in head:
                    head["name"] = rev_result["name"]
                    del rev_result["name"]
                if rev_result["executor"]:
                    if not head["executor"]:
                        head["executor"] = rev_result["executor"]
                    rev_result["executor"] = None
                if rev_result["status"] == ExpStatus.Running.name:
                    head["status"] = ExpStatus.Running.name
                    rev_result["status"] = ExpStatus.Success.name
            elif rev_result["checkpoint_tip"] == rev:
                head = rev_result
                checkpoint = True


def show(  # noqa: PLR0913
    repo: "Repo",
    all_branches=False,
    all_tags=False,
    revs: Union[List[str], str, None] = None,
    all_commits=False,
    hide_queued=False,
    hide_failed=False,
    sha_only=False,
    num=1,
    param_deps=False,
    onerror: Optional[Callable] = None,
    fetch_running: bool = True,
    force: bool = False,
):
    if repo.scm.no_commits:
        return {}

    res: Dict[str, Dict] = defaultdict(OrderedDict)

    if not any([revs, all_branches, all_tags, all_commits]):
        revs = ["HEAD"]
    if isinstance(revs, str):
        revs = [revs]

    assert isinstance(repo.scm, Git)

    found_revs: Dict[str, List[str]] = {"workspace": []}
    found_revs.update(
        iter_revs(repo.scm, revs, num, all_branches, all_tags, all_commits)
    )
    cached_refs = list(repo.scm.iter_refs())
    branch_names = get_branch_names(repo.scm, found_revs, refs=cached_refs)

    running: Dict[str, Dict] = repo.experiments.get_running_exps(
        fetch_refs=fetch_running
    )

    queued_experiment = (
        _collect_queued_experiment(
            repo,
            found_revs,
            running,
            param_deps=param_deps,
            force=force,
        )
        if not hide_queued
        else {}
    )

    active_experiment = _collect_active_experiment(
        repo,
        found_revs,
        running,
        param_deps=param_deps,
        onerror=onerror,
        force=force,
    )

    failed_experiments = (
        _collect_failed_experiment(
            repo,
            found_revs,
            running,
            param_deps=param_deps,
            onerror=onerror,
            force=force,
        )
        if not hide_failed
        else {}
    )

    for baseline in found_revs:
        res[baseline] = _collect_branch(
            repo,
            baseline,
            running=running,
            param_deps=param_deps,
            onerror=onerror,
            force=force,
            refs=cached_refs,
        )

    update_new(res, failed_experiments)

    update_new(res, active_experiment)

    update_new(res, queued_experiment)

    if not sha_only:
        update_names(repo, branch_names, res)

    move_properties_to_head(res)

    return res
