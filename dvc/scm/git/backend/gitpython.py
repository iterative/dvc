import logging
from typing import Callable, Iterable, Optional, Tuple

from .base import BaseGitBackend

logger = logging.getLogger(__name__)


class GitPythonBackend(BaseGitBackend):  # pylint:disable=abstract-method
    """git-python Git backend."""

    @property
    def git(self):
        return self.scm.repo.git

    def is_ignored(self, path):
        raise NotImplementedError

    def set_ref(
        self,
        name: str,
        new_ref: str,
        old_ref: Optional[str] = None,
        message: Optional[str] = None,
        symbolic: Optional[bool] = False,
    ):
        raise NotImplementedError

    def get_ref(self, name, follow: Optional[bool] = True) -> Optional[str]:
        raise NotImplementedError

    def remove_ref(self, name: str, old_ref: Optional[str] = None):
        raise NotImplementedError

    def iter_refs(self, base: Optional[str] = None):
        from git import Reference

        for ref in Reference.iter_items(self.scm.repo, common_path=base):
            if base and base.endswith("/"):
                base = base[:-1]
            yield "/".join([base, ref.name])

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None):
        from git.exc import GitCommandError

        try:
            if pattern:
                args = [pattern]
            else:
                args = []
            for line in self.git.for_each_ref(
                *args, contains=rev, format=r"%(refname)"
            ).splitlines():
                line = line.strip()
                if line:
                    yield line
        except GitCommandError:
            pass

    def push_refspec(self, url: str, src: Optional[str], dest: str):
        raise NotImplementedError

    def fetch_refspecs(
        self,
        url: str,
        refspecs: Iterable[str],
        force: Optional[bool] = False,
        on_diverged: Optional[Callable[[bytes, bytes], bool]] = None,
    ):
        raise NotImplementedError

    def _stash_iter(self, ref: str):
        raise NotImplementedError

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

    def describe(
        self,
        rev: str,
        base: Optional[str] = None,
        match: Optional[str] = None,
        exclude: Optional[str] = None,
    ) -> Optional[str]:
        raise NotImplementedError

    def diff(self, rev_a: str, rev_b: str, binary=False) -> str:
        raise NotImplementedError
