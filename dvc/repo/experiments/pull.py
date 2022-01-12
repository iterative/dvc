import logging
from typing import Iterable, Optional, Set, Union

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit, iter_revs

from .base import ExpRefInfo
from .exceptions import UnresolvedExpNamesError
from .utils import exp_commits, exp_refs, exp_refs_by_baseline, resolve_name

logger = logging.getLogger(__name__)


@locked
@scm_context
def pull(
    repo,
    git_remote: str,
    exp_names: Union[Iterable[str], str],
    all_commits=False,
    rev: Optional[str] = None,
    num=1,
    force: bool = False,
    pull_cache: bool = False,
    **kwargs,
) -> Iterable[str]:
    exp_ref_set: Set["ExpRefInfo"] = set()
    if all_commits:
        exp_ref_set.update(exp_refs(repo.scm, git_remote))
    else:
        if exp_names:
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

        if rev:
            rev_dict = iter_revs(repo.scm, [rev], num)
            rev_set = set(rev_dict.keys())
            ref_info_dict = exp_refs_by_baseline(repo.scm, rev_set, git_remote)
            for _, ref_info_list in ref_info_dict.items():
                exp_ref_set.update(ref_info_list)

    _pull(repo, git_remote, exp_ref_set, force)
    if pull_cache:
        _pull_cache(repo, exp_ref_set, **kwargs)
    return [ref.name for ref in exp_ref_set]


def _pull(
    repo,
    git_remote: str,
    refs,
    force: bool,
):
    def on_diverged(refname: str, rev: str) -> bool:
        if repo.scm.get_ref(refname) == rev:
            return True
        exp_name = refname.split("/")[-1]
        raise DvcException(
            f"Local experiment '{exp_name}' has diverged from remote "
            "experiment with the same name. To override the local experiment "
            "re-run with '--force'."
        )

    refspec_list = [f"{exp_ref}:{exp_ref}" for exp_ref in refs]
    logger.debug(f"git pull experiment '{git_remote}' -> '{refspec_list}'")

    with TqdmGit(desc="Fetching git refs") as pbar:
        repo.scm.fetch_refspecs(
            git_remote,
            refspec_list,
            force=force,
            on_diverged=on_diverged,
            progress=pbar.update_git,
        )


def _pull_cache(
    repo,
    refs: Union[ExpRefInfo, Iterable["ExpRefInfo"]],
    dvc_remote=None,
    jobs=None,
    run_cache=False,
    odb=None,
):
    if isinstance(refs, ExpRefInfo):
        refs = [refs]
    revs = list(exp_commits(repo.scm, refs))
    logger.debug(f"dvc fetch experiment '{refs}'")
    repo.fetch(
        jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs, odb=odb
    )
