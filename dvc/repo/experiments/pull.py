from collections.abc import Iterable, Mapping
from typing import Optional, Union

from funcy import group_by
from scmrepo.git.backend.base import SyncStatus

from dvc.log import logger
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit, iter_revs
from dvc.ui import ui

from .exceptions import UnresolvedExpNamesError
from .refs import ExpRefInfo
from .utils import exp_commits, exp_refs, exp_refs_by_baseline, resolve_name

logger = logger.getChild(__name__)


@locked
@scm_context
def pull(  # noqa: C901
    repo,
    git_remote: str,
    exp_names: Optional[Union[Iterable[str], str]] = None,
    all_commits=False,
    rev: Optional[Union[list[str], str]] = None,
    num=1,
    force: bool = False,
    pull_cache: bool = False,
    **kwargs,
) -> Iterable[str]:
    exp_ref_set: set[ExpRefInfo] = set()
    if all_commits:
        exp_ref_set.update(exp_refs(repo.scm, git_remote))
    elif exp_names:
        if isinstance(exp_names, str):
            exp_names = [exp_names]
        exp_ref_dict = resolve_name(repo.scm, exp_names, git_remote)

        unresolved_exp_names = []
        for exp_name, exp_ref in exp_ref_dict.items():
            if exp_ref is None:
                unresolved_exp_names.append(exp_name)
            else:
                exp_ref_set.add(exp_ref)

        if unresolved_exp_names:
            raise UnresolvedExpNamesError(unresolved_exp_names)

    else:
        rev = rev or "HEAD"
        if isinstance(rev, str):
            rev = [rev]
        rev_dict = iter_revs(repo.scm, rev, num)
        rev_set = set(rev_dict.keys())
        ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)
        for _, ref_info_list in ref_info_dict.items():
            exp_ref_set.update(ref_info_list)

    pull_result = _pull(repo, git_remote, exp_ref_set, force)

    if pull_result[SyncStatus.DIVERGED]:
        diverged_refs = [ref.name for ref in pull_result[SyncStatus.DIVERGED]]
        ui.warn(
            f"Local experiment '{diverged_refs}' has diverged from remote "
            "experiment with the same name. To override the local experiment "
            "re-run with '--force'."
        )

    if pull_cache:
        pull_cache_ref = (
            pull_result[SyncStatus.UP_TO_DATE] + pull_result[SyncStatus.SUCCESS]
        )
        _pull_cache(repo, pull_cache_ref, **kwargs)

    return [ref.name for ref in pull_result[SyncStatus.SUCCESS]]


def _pull(
    repo,
    git_remote: str,
    refs: Iterable["ExpRefInfo"],
    force: bool,
) -> Mapping[SyncStatus, list["ExpRefInfo"]]:
    refspec_list = [f"{exp_ref}:{exp_ref}" for exp_ref in refs]
    logger.debug("git pull experiment '%s' -> '%s'", git_remote, refspec_list)

    with TqdmGit(desc="Fetching git refs") as pbar:
        results: Mapping[str, SyncStatus] = repo.scm.fetch_refspecs(
            git_remote,
            refspec_list,
            force=force,
            progress=pbar.update_git,
        )

    def group_result(refspec):
        return results[str(refspec)]

    pull_result: Mapping[SyncStatus, list[ExpRefInfo]] = group_by(group_result, refs)

    return pull_result


def _pull_cache(
    repo,
    refs: Union[ExpRefInfo, Iterable["ExpRefInfo"]],
    dvc_remote=None,
    jobs=None,
    run_cache=False,
):
    if isinstance(refs, ExpRefInfo):
        refs = [refs]
    revs = list(exp_commits(repo.scm, refs))
    logger.debug("dvc fetch experiment '%s'", refs)
    repo.fetch(
        jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs, workspace=False
    )
