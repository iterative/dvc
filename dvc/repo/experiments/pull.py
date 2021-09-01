import logging

from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .utils import exp_commits, resolve_exp_ref

logger = logging.getLogger(__name__)


@locked
@scm_context
def pull(
    repo, git_remote, exp_name, *args, force=False, pull_cache=False, **kwargs
):
    exp_ref = resolve_exp_ref(repo.scm, exp_name, git_remote)
    if not exp_ref:
        raise InvalidArgumentError(
            f"Experiment '{exp_name}' does not exist in '{git_remote}'"
        )

    def on_diverged(refname: str, rev: str) -> bool:
        if repo.scm.get_ref(refname) == rev:
            return True
        raise DvcException(
            f"Local experiment '{exp_name}' has diverged from remote "
            "experiment with the same name. To override the local experiment "
            "re-run with '--force'."
        )

    refspec = f"{exp_ref}:{exp_ref}"
    logger.debug("git pull experiment '%s' -> '%s'", git_remote, refspec)
    repo.scm.fetch_refspecs(
        git_remote, [refspec], force=force, on_diverged=on_diverged
    )

    if pull_cache:
        _pull_cache(repo, exp_ref, **kwargs)


def _pull_cache(repo, exp_ref, dvc_remote=None, jobs=None, run_cache=False):
    revs = list(exp_commits(repo.scm, [exp_ref]))
    logger.debug("dvc fetch experiment '%s'", exp_ref)
    repo.fetch(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
