import logging

from dvc.exceptions import DvcException, InvalidArgumentError
from dvc.repo import locked
from dvc.repo.scm_context import scm_context

from .utils import exp_commits, push_refspec, resolve_exp_ref

logger = logging.getLogger(__name__)


@locked
@scm_context
def push(
    repo,
    git_remote,
    exp_name: str,
    *args,
    force=False,
    push_cache=False,
    **kwargs,
):
    exp_ref = resolve_exp_ref(repo.scm, exp_name)
    if not exp_ref:
        raise InvalidArgumentError(
            f"'{exp_name}' is not a valid experiment name"
        )

    def on_diverged(refname: str, rev: str) -> bool:
        if repo.scm.get_ref(refname) == rev:
            return True
        raise DvcException(
            f"Local experiment '{exp_name}' has diverged from remote "
            "experiment with the same name. To override the remote experiment "
            "re-run with '--force'."
        )

    refname = str(exp_ref)
    logger.debug("git push experiment '%s' -> '%s'", exp_ref, git_remote)

    from dvc.scm import TqdmGit

    with TqdmGit(desc="Pushing git refs") as pbar:
        push_refspec(
            repo.scm,
            git_remote,
            refname,
            refname,
            force=force,
            on_diverged=on_diverged,
            progress=pbar.update_git,
        )

    if push_cache:
        _push_cache(repo, exp_ref, **kwargs)


def _push_cache(repo, exp_ref, dvc_remote=None, jobs=None, run_cache=False):
    revs = list(exp_commits(repo.scm, [exp_ref]))
    logger.debug("dvc push experiment '%s'", exp_ref)
    repo.push(jobs=jobs, remote=dvc_remote, run_cache=run_cache, revs=revs)
