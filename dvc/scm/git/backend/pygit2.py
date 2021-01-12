import locale
import logging
import os
from io import BytesIO, StringIO
from typing import Callable, Iterable, List, Mapping, Optional, Tuple

from dvc.scm.base import MergeConflictError, SCMError
from dvc.utils import relpath

from ..objects import GitObject
from .base import BaseGitBackend

logger = logging.getLogger(__name__)


class Pygit2Object(GitObject):
    def __init__(self, obj):
        self.obj = obj

    def open(self, mode: str = "r", encoding: str = None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        data = self.obj.read_raw()
        if mode == "rb":
            return BytesIO(data)
        return StringIO(data.decode(encoding))

    @property
    def name(self) -> str:
        return self.obj.name

    @property
    def mode(self):
        return self.obj.filemode

    def scandir(self) -> Iterable["Pygit2Object"]:
        for entry in self.obj:  # noqa: B301
            yield Pygit2Object(entry)


class Pygit2Backend(BaseGitBackend):  # pylint:disable=abstract-method
    def __init__(  # pylint:disable=W0231
        self, root_dir=os.curdir, search_parent_directories=True
    ):
        import pygit2

        if search_parent_directories:
            ceiling_dirs = ""
        else:
            ceiling_dirs = os.path.abspath(root_dir)

        # NOTE: discover_repository will return path/.git/
        path = pygit2.discover_repository(  # pylint:disable=no-member
            root_dir, True, ceiling_dirs
        )
        if not path:
            raise SCMError(f"{root_dir} is not a git repository")

        self.repo = pygit2.Repository(path)

        self._stashes: dict = {}

    def close(self):
        self.repo.free()

    @property
    def root_dir(self) -> str:
        return self.repo.workdir

    @staticmethod
    def clone(
        url: str,
        to_path: str,
        rev: Optional[str] = None,
        shallow_branch: Optional[str] = None,
    ):
        raise NotImplementedError

    @staticmethod
    def is_sha(rev: str) -> bool:
        raise NotImplementedError

    @property
    def dir(self) -> str:
        raise NotImplementedError

    def add(self, paths: Iterable[str], update=False):
        raise NotImplementedError

    def commit(self, msg: str, no_verify: bool = False):
        raise NotImplementedError

    def checkout(
        self, branch: str, create_new: Optional[bool] = False, **kwargs,
    ):
        raise NotImplementedError

    def pull(self, **kwargs):
        raise NotImplementedError

    def push(self):
        raise NotImplementedError

    def branch(self, branch: str):
        raise NotImplementedError

    def tag(self, tag: str):
        raise NotImplementedError

    def untracked_files(self) -> Iterable[str]:
        raise NotImplementedError

    def is_tracked(self, path: str) -> bool:
        raise NotImplementedError

    def is_dirty(self, **kwargs) -> bool:
        raise NotImplementedError

    def active_branch(self) -> str:
        raise NotImplementedError

    def list_branches(self) -> Iterable[str]:
        raise NotImplementedError

    def list_tags(self) -> Iterable[str]:
        raise NotImplementedError

    def list_all_commits(self) -> Iterable[str]:
        raise NotImplementedError

    def get_tree_obj(self, rev: str, **kwargs) -> Pygit2Object:
        tree = self.repo[rev].tree
        return Pygit2Object(tree)

    def get_rev(self) -> str:
        raise NotImplementedError

    def resolve_rev(self, rev: str) -> str:
        raise NotImplementedError

    def resolve_commit(self, rev: str) -> str:
        raise NotImplementedError

    def branch_revs(self, branch: str, end_rev: Optional[str] = None):
        raise NotImplementedError

    def _get_stash(self, ref: str):
        raise NotImplementedError

    def is_ignored(self, path: str) -> bool:
        return self.repo.path_is_ignored(relpath(path, self.root_dir))

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
        raise NotImplementedError

    def get_refs_containing(self, rev: str, pattern: Optional[str] = None):
        raise NotImplementedError

    def push_refspec(
        self,
        url: str,
        src: Optional[str],
        dest: str,
        force: bool = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
    ):
        raise NotImplementedError

    def fetch_refspecs(
        self,
        url: str,
        refspecs: Iterable[str],
        force: Optional[bool] = False,
        on_diverged: Optional[Callable[[str, str], bool]] = None,
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
        raise NotImplementedError

    def _stash_apply(self, rev: str):
        raise NotImplementedError

    def reflog_delete(
        self, ref: str, updateref: bool = False, rewrite: bool = False
    ):
        raise NotImplementedError

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

    def reset(self, hard: bool = False, paths: Iterable[str] = None):
        raise NotImplementedError

    def checkout_index(
        self,
        paths: Optional[Iterable[str]] = None,
        force: bool = False,
        ours: bool = False,
        theirs: bool = False,
    ):
        from pygit2 import (
            GIT_CHECKOUT_ALLOW_CONFLICTS,
            GIT_CHECKOUT_FORCE,
            GIT_CHECKOUT_RECREATE_MISSING,
            GIT_CHECKOUT_SAFE,
        )

        assert not (ours and theirs)
        strategy = GIT_CHECKOUT_RECREATE_MISSING
        if force or ours or theirs:
            strategy |= GIT_CHECKOUT_FORCE
        else:
            strategy |= GIT_CHECKOUT_SAFE

        if ours or theirs:
            strategy |= GIT_CHECKOUT_ALLOW_CONFLICTS

        index = self.repo.index
        if paths:
            path_list: Optional[List[str]] = [
                relpath(path, self.root_dir) for path in paths
            ]
        else:
            path_list = None
        self.repo.checkout_index(
            index=index, paths=path_list, strategy=strategy,
        )

        if index.conflicts and (ours or theirs):
            for ancestor, ours_entry, theirs_entry in index.conflicts:
                if not ancestor:
                    continue
                if ours:
                    entry = ours_entry
                    index.add(ours_entry)
                else:
                    entry = theirs_entry
                path = os.path.join(self.root_dir, entry.path)
                with open(path, "wb") as fobj:
                    fobj.write(self.repo.get(entry.id).read_raw())
                index.add(entry.path)
            index.write()

    def iter_remote_refs(self, url: str, base: Optional[str] = None):
        raise NotImplementedError

    def status(
        self, ignored: bool = False
    ) -> Tuple[Mapping[str, Iterable[str]], Iterable[str], Iterable[str]]:
        raise NotImplementedError

    def merge(
        self,
        rev: str,
        commit: bool = True,
        msg: Optional[str] = None,
        squash: bool = False,
    ) -> Optional[str]:
        from pygit2 import GIT_RESET_MIXED, GitError

        if commit and squash:
            raise SCMError("Cannot merge with 'squash' and 'commit'")

        if commit and not msg:
            raise SCMError("Merge commit message is required")

        try:
            self.repo.merge(rev)
        except GitError as exc:
            raise SCMError("Merge failed") from exc

        if self.repo.index.conflicts:
            raise MergeConflictError("Merge contained conflicts")

        if commit:
            user = self.repo.default_signature
            tree = self.repo.index.write_tree()
            merge_commit = self.repo.create_commit(
                "HEAD", user, user, msg, tree, [self.repo.head.target, rev]
            )
            return str(merge_commit)
        if squash:
            self.repo.reset(self.repo.head.target, GIT_RESET_MIXED)
            self.repo.state_cleanup()
        return None
