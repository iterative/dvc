import logging
import os

from dvc.exceptions import InvalidArgumentError
from dvc.repo import locked
from dvc.repo.experiments.executor import BaseExecutor
from dvc.repo.scm_context import scm_context
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


class ApplyError(InvalidArgumentError):
    def __init__(self, rev):
        super().__init__(
            f"'{rev}' does not appear to be an experiment commit."
        )


@locked
@scm_context
def apply(repo, rev, *args, **kwargs):
    from git.exc import GitCommandError

    from dvc.repo.checkout import checkout as dvc_checkout
    from dvc.repo.experiments import BaselineMismatchError

    exps = repo.experiments

    try:
        exps.check_baseline(rev)
    except BaselineMismatchError as exc:
        raise ApplyError(rev) from exc

    stash_rev = rev in exps.stash_revs
    if stash_rev:
        branch = rev
    else:
        branch = exps.get_branch_containing(rev)
        if not branch:
            raise ApplyError(rev)

    # Note that we don't use stash_workspace() here since we need finer control
    # over the merge behavior when we unstash everything
    if repo.scm.is_dirty(untracked_files=True):
        logger.debug("Stashing workspace")
        workspace = repo.scm.stash.push(include_untracked=True)
    else:
        workspace = None

    repo.scm.repo.git.merge(branch, squash=True, no_commit=True)

    if workspace:
        try:
            repo.scm.stash.apply(workspace)
        except GitCommandError:
            # if stash apply returns merge conflicts, prefer experiment
            # changes over prior stashed changes
            repo.scm.repo.git.checkout("--ours", "--", ".")
        repo.scm.stash.drop()
    repo.scm.repo.git.reset()

    if stash_rev:
        args_path = os.path.join(repo.tmp_dir, BaseExecutor.PACKED_ARGS_FILE)
        if os.path.exists(args_path):
            remove(args_path)

    dvc_checkout(repo, **kwargs)

    logger.info(
        "Changes for experiment '%s' have been applied to your current "
        "workspace.",
        rev,
    )
