import logging
from typing import Optional, Tuple

from .base import BaseGitBackend

logger = logging.getLogger(__name__)


class GitPythonBackend(BaseGitBackend):  # pylint:disable=abstract-method
    """git-python Git backend."""

    @property
    def git(self):
        return self.scm.repo.git

    def _stash_push(
        self,
        ref: str,
        message: Optional[str] = None,
        include_untracked: Optional[bool] = False,
    ) -> Tuple[Optional[str], bool]:
        from dvc.scm.git import Stash

        args = ["push"]
        if message:
            args.extend(["-m", message])
        if include_untracked:
            args.append("--include-untracked")
        self.git.stash(*args)
        commit = self.scm.resolve_commit("stash@{0}")
        if ref != Stash.DEFAULT_STASH:
            # `git stash` CLI doesn't support using custom refspecs,
            # so we push a commit onto refs/stash, make our refspec
            # point to the new commit, then pop it from refs/stash
            # `git stash create` is intended to be used for this kind of
            # behavior but it doesn't support --include-untracked so we need to
            # use push
            self.scm.set_ref(ref, commit.hexsha, message=commit.message)
            self.git.stash("drop")
        return commit.hexsha, False

    def _stash_apply(self, rev: str):
        self.git.stash("apply", rev)

    def reflog_delete(self, ref: str, updateref: bool = False):
        args = ["delete"]
        if updateref:
            args.append("--updateref")
        args.append(ref)
        self.git.reflog(*args)
