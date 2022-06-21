import logging
from typing import Iterable, List, Mapping, Optional, Set, Union

from funcy import group_by
from scmrepo.git.backend.base import SyncStatus

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit, iter_revs
from dvc.ui import ui

from .base import ExpRefInfo
from .exceptions import UnresolvedExpNamesError
from .utils import exp_commits, exp_refs, exp_refs_by_baseline, resolve_name

logger = logging.getLogger(__name__)


@locked
@scm_context
def push(
    repo,
    git_remote: str,
    exp_names: Union[Iterable[str], str],
    all_commits=False,
    rev: Optional[str] = None,
    num=1,
    force: bool = False,
    push_cache: bool = False,
    **kwargs,
) -> Iterable[str]:

    exp_ref_set: Set["ExpRefInfo"] = set()
    if all_commits:
        exp_ref_set.update(exp_refs(repo.scm))

    else:
        if exp_names:
            if isinstance(exp_names, str):
                exp_names = [exp_names]
            exp_ref_dict = resolve_name(repo.scm, exp_names)

            unresolved_exp_names = []
            for exp_name, exp_ref in exp_ref_dict.items():
                if exp_ref is None:
                    unresolved_exp_names.append(exp_name)
                else:
                    exp_ref_set.add(exp_ref)

            if unresolved_exp_names:
                raise UnresolvedExpNamesError(unresolved_exp_names)

        if rev:
            rev_dict = iter_revs(repo.scm, [rev], num)
            rev_set = set(rev_dict.keys())
            ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set)
            for _, ref_info_list in ref_info_dict.items():
                exp_ref_set.update(ref_info_list)

    push_result = _push(repo, git_remote, exp_ref_set, force)
    if push_result[SyncStatus.DIVERGED]:
        diverged_refs = [ref.name for ref in push_result[SyncStatus.DIVERGED]]
        ui.warn(
            f"Local experiment '{diverged_refs}' has diverged from remote "
            "experiment with the same name. To override the remote experiment "
            "re-run with '--force'."
        )
    if push_cache:
        push_cache_ref = (
            push_result[SyncStatus.UP_TO_DATE]
            + push_result[SyncStatus.SUCCESS]
        )
        _push_cache(repo, push_cache_ref, **kwargs)
    return [ref.name for ref in push_result[SyncStatus.SUCCESS]]


def _push(
    repo,
    git_remote: str,
    refs: Iterable["ExpRefInfo"],
    force: bool,
) -> Mapping[SyncStatus, List["ExpRefInfo"]]:
    from scmrepo.exceptions import AuthError

    from ...scm import GitAuthError

    refspec_list = [f"{exp_ref}:{exp_ref}" for exp_ref in refs]
    logger.debug(f"git push experiment '{refs}' -> '{git_remote}'")

    with TqdmGit(desc="Pushing git refs") as pbar:
        try:
            results: Mapping[str, SyncStatus] = repo.scm.push_refspecs(
                git_remote,
                refspec_list,
                force=force,
                progress=pbar.update_git,
            )
        except AuthError as exc:
            raise GitAuthError(str(exc))

    def group_result(refspec):
        return results[str(refspec)]

    pull_result: Mapping[SyncStatus, List["ExpRefInfo"]] = group_by(
        group_result, refs
    )

    return pull_result


def _push_cache(
    repo,
    refs: Union[ExpRefInfo, Iterable["ExpRefInfo"]],
    dvc_remote=None,
    jobs=None,
    run_cache=False,
):
    if isinstance(refs, ExpRefInfo):
        refs = [refs]
    revs = list(exp_commits(repo.scm, refs))
    logger.debug(f"dvc push experiment '{refs}'")
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
