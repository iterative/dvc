import logging

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit
from dvc.types import List, Optional, Union

from .base import ExpRefInfo
from .exceptions import UnresolvedExpNamesError
from .utils import (
    exp_commits,
    get_exp_ref_from_variables,
    name2exp_ref,
    push_refspec,
)

logger = logging.getLogger(__name__)


@locked
@scm_context
def push(
    repo,
    git_remote: str,
    exp_names: Union[List[str], str],
    *args,
    all_: bool = False,
    rev: Optional[str] = None,
    branch: Optional[str] = None,
    max_count: int = 1,
    force: bool = False,
    push_cache: bool = False,
    **kwargs,
):
    if not exp_names:
        exp_names = []
        for info in get_exp_ref_from_variables(
            repo.scm, rev, all_, branch, max_count, None
        ):
            exp_names.append(info.name)

    if isinstance(exp_names, str):
        exp_names = [exp_names]
    exp_ref_list, unresolved_exp_names = name2exp_ref(
        repo.scm, exp_names, None, **kwargs
    )
    if unresolved_exp_names:
        raise UnresolvedExpNamesError(unresolved_exp_names)

    _push(repo, git_remote, exp_ref_list, force, push_cache, **kwargs)


def _push_cache(repo, exp_refs, dvc_remote=None, jobs=None, run_cache=False):
    revs = list(exp_commits(repo.scm, exp_refs))
    logger.debug(f"dvc push experiment '{exp_refs}'")
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)


def _push(
    repo,
    git_remote: str,
    exp_refs: List[ExpRefInfo],
    force: bool,
    push_cache: bool,
    **kwargs,
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

    logger.debug(f"git push experiment '{exp_refs}' -> '{git_remote}'")

    for exp_ref in exp_refs:
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

    if push_cache:
        _push_cache(repo, exp_refs, **kwargs)
