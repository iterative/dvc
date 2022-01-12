import logging
from typing import Iterable, Optional, Set, Union

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit, iter_revs

from .base import ExpRefInfo
from .exceptions import UnresolvedExpNamesError
from .utils import (
    exp_commits,
    exp_refs,
    exp_refs_by_baseline,
    push_refspec,
    resolve_name,
)

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

    _push(repo, git_remote, exp_ref_set, force)
    if push_cache:
        _push_cache(repo, exp_ref_set, **kwargs)
    return [ref.name for ref in exp_ref_set]


def _push(
    repo,
    git_remote: str,
    refs: Iterable["ExpRefInfo"],
    force: bool,
):
    def on_diverged(refname: str, rev: str) -> bool:
        if repo.scm.get_ref(refname) == rev:
            return True
        exp_name = refname.split("/")[-1]
        raise DvcException(
            f"Local experiment '{exp_name}' has diverged from remote "
            "experiment with the same name. To override the remote experiment "
            "re-run with '--force'."
        )

    logger.debug(f"git push experiment '{refs}' -> '{git_remote}'")

    for exp_ref in refs:
        with TqdmGit(desc="Pushing git refs") as pbar:
            push_refspec(
                repo.scm,
                git_remote,
                str(exp_ref),
                str(exp_ref),
                force=force,
                on_diverged=on_diverged,
                progress=pbar.update_git,
            )


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
