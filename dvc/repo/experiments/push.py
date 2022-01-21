import logging
from typing import Iterable, Union

from dvc.exceptions import DvcException
from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm import TqdmGit

from .exceptions import UnresolvedExpNamesError
from .utils import exp_commits, push_refspec, resolve_name

logger = logging.getLogger(__name__)


@locked
@scm_context
def push(
    repo,
    git_remote: str,
    exp_names: Union[Iterable[str], str],
    *args,
    force: bool = False,
    push_cache: bool = False,
    **kwargs,
):
    if isinstance(exp_names, str):
        exp_names = [exp_names]

    exp_ref_dict = resolve_name(repo.scm, exp_names)
    unresolved_exp_names = [
        exp_name
        for exp_name, exp_ref in exp_ref_dict.items()
        if exp_ref is None
    ]
    if unresolved_exp_names:
        raise UnresolvedExpNamesError(unresolved_exp_names)

    exp_ref_set = exp_ref_dict.values()
    _push(repo, git_remote, exp_ref_set, force, push_cache, **kwargs)


def _push(
    repo,
    git_remote: str,
    exp_refs,
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


def _push_cache(repo, exp_refs, dvc_remote=None, jobs=None, run_cache=False):
    revs = list(exp_commits(repo.scm, exp_refs))
    logger.debug(f"dvc push experiment '{exp_refs}'")
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
