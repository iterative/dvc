import logging
from typing import Iterable, Union

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit

from .exceptions import UnresolvedExpNamesError
from .utils import exp_commits, resolve_name

logger = logging.getLogger(__name__)


@locked
@scm_context
def pull(
    repo,
    git_remote: str,
    exp_names: Union[Iterable[str], str],
    *args,
    force: bool = False,
    pull_cache: bool = False,
    **kwargs,
):
    if isinstance(exp_names, str):
        exp_names = [exp_names]
    exp_ref_dict = resolve_name(repo.scm, exp_names, git_remote)
    unresolved_exp_names = [
        exp_name
        for exp_name, exp_ref in exp_ref_dict.items()
        if exp_ref is None
    ]
    if unresolved_exp_names:
        raise UnresolvedExpNamesError(unresolved_exp_names)

    exp_ref_set = exp_ref_dict.values()
    _pull(repo, git_remote, exp_ref_set, force, pull_cache, **kwargs)


def _pull(
    repo,
    git_remote: str,
    exp_refs,
    force: bool,
    pull_cache: bool,
    **kwargs,
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

    refspec_list = [f"{exp_ref}:{exp_ref}" for exp_ref in exp_refs]
    logger.debug(f"git pull experiment '{git_remote}' -> '{refspec_list}'")

    with TqdmGit(desc="Fetching git refs") as pbar:
        repo.scm.fetch_refspecs(
            git_remote,
            refspec_list,
            force=force,
            on_diverged=on_diverged,
            progress=pbar.update_git,
        )

    if pull_cache:
        _pull_cache(repo, exp_refs, **kwargs)


def _pull_cache(
    repo,
    exp_ref,
    dvc_remote=None,
    jobs=None,
    run_cache=False,
    odb=None,
):
    revs = list(exp_commits(repo.scm, exp_ref))
    logger.debug(f"dvc fetch experiment '{exp_ref}'")
    repo.fetch(
        jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs, odb=odb
    )
