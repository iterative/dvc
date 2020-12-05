import logging
import os

from dvc.repo import locked
from dvc.repo.scm_context import scm_context
from dvc.scm.base import RevError
from dvc.utils.fs import remove

from .base import BaselineMismatchError, InvalidExpRevError
from .executor import BaseExecutor

logger = logging.getLogger(__name__)


@locked
@scm_context
def apply(repo, rev, *args, **kwargs):
    from git.exc import GitCommandError

    from dvc.repo.checkout import checkout as dvc_checkout

    exps = repo.experiments

    try:
        rev = repo.scm.resolve_rev(rev)
        exps.check_baseline(rev)
    except (RevError, BaselineMismatchError) as exc:
        raise InvalidExpRevError(rev) from exc

    stash_rev = rev in exps.stash_revs
    if stash_rev:
        branch = rev
    else:
        branch = exps.get_branch_by_rev(rev)
        if not branch:
            raise InvalidExpRevError(rev)

    # Note that we don't use stash_workspace() here since we need finer control
    # over the merge behavior when we unstash everything
    if repo.scm.is_dirty(untracked_files=True):
        logger.debug("Stashing workspace")
        workspace = repo.scm.stash.push(include_untracked=True)
    else:
        workspace = None

    repo.scm.gitpython.repo.git.merge(branch, squash=True, no_commit=True)

    if workspace:
        try:
            repo.scm.stash.apply(workspace)
        except GitCommandError:
            # if stash apply returns merge conflicts, prefer experiment
            # changes over prior stashed changes
            repo.scm.gitpython.repo.git.checkout("--ours", "--", ".")
        repo.scm.stash.drop()
    repo.scm.gitpython.repo.git.reset()

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
