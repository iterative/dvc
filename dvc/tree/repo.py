import logging
import os
import stat
import threading
from contextlib import suppress
from itertools import takewhile
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Union

from funcy import lfilter, wrap_with
from pygtrie import StringTrie

from dvc.dvcfile import is_valid_filename
from dvc.hash_info import HashInfo
from dvc.path_info import PathInfo
from dvc.utils import file_md5, is_exec
from dvc.utils.fs import copy_fobj_to_file, makedirs

from ._metadata import Metadata
from .base import BaseTree
from .dvc import DvcTree

if TYPE_CHECKING:
    from dvc.repo import Repo

    from .git import GitTree
    from .local import LocalTree


logger = logging.getLogger(__name__)


class RepoTree(BaseTree):  # pylint:disable=abstract-method
    """DVC + git-tracked files tree.

    Args:
        repo: DVC or git repo.
        subrepos: traverse to subrepos (by default, it ignores subrepos)
        repo_factory: A function to initialize subrepo with, default is Repo.
        kwargs: Additional keyword arguments passed to the `DvcTree()`.
    """

    scheme = "local"
    PARAM_CHECKSUM = "md5"

    def __init__(
        self,
        repo,
        subrepos=False,
        repo_factory: Callable[[str], "Repo"] = None,
        **kwargs
    ):
        super().__init__(repo, {"url": repo.root_dir})

        if not repo_factory:
            from dvc.repo import Repo

            self.repo_factory = Repo
        else:
            self.repo_factory = repo_factory

        self._main_repo = repo
        self.root_dir = repo.root_dir
        self._traverse_subrepos = subrepos

        self._subrepos_trie = StringTrie(separator=os.sep)
        """Keeps track of each and every path with the corresponding repo."""

        self._subrepos_trie[self.root_dir] = repo

        self._dvctrees = {}
        """Keep a dvctree instance of each repo."""

        self._dvctree_configs = kwargs

        if hasattr(repo, "dvc_dir"):
            self._dvctrees[repo.root_dir] = DvcTree(repo, **kwargs)

    def _get_repo(self, path) -> Optional["Repo"]:
        """Returns repo that the path falls in, using prefix.

        If the path is already tracked/collected, it just returns the repo.

        Otherwise, it collects the repos that might be in the path's parents
        and then returns the appropriate one.
        """
        repo = self._subrepos_trie.get(path)
        if repo:
            return repo

        prefix, repo = self._subrepos_trie.longest_prefix(path)
        if not prefix:
            return None

        parents = (parent.fspath for parent in PathInfo(path).parents)
        dirs = [path] + list(takewhile(lambda p: p != prefix, parents))
        dirs.reverse()
        self._update(dirs, starting_repo=repo)
        return self._subrepos_trie.get(path)

    @wrap_with(threading.Lock())
    def _update(self, dirs, starting_repo):
        """Checks for subrepo in directories and updates them."""
        repo = starting_repo
        for d in dirs:
            if self._is_dvc_repo(d):
                repo = self.repo_factory(d)
                self._dvctrees[repo.root_dir] = DvcTree(
                    repo, **self._dvctree_configs
                )
            self._subrepos_trie[d] = repo

    def _is_dvc_repo(self, dir_path):
        """Check if the directory is a dvc repo."""
        if not self._traverse_subrepos:
            return False

        from dvc.repo import Repo

        repo_path = os.path.join(dir_path, Repo.DVC_DIR)
        # dvcignore will ignore subrepos, therefore using `use_dvcignore=False`
        return self._main_repo.tree.isdir(repo_path, use_dvcignore=False)

    def _get_tree_pair(
        self, path
    ) -> Tuple[Union["GitTree", "LocalTree"], DvcTree]:
        """
        Returns a pair of trees based on repo the path falls in, using prefix.
        """
        path = os.path.abspath(path)

        # fallback to the top-level repo if repo was not found
        # this can happen if the path is outside of the repo
        repo = self._get_repo(path) or self._main_repo

        dvc_tree = self._dvctrees.get(repo.root_dir)
        return repo.tree, dvc_tree

    @property
    def fetch(self):
        return "fetch" in self._dvctree_configs

    @property
    def stream(self):
        return "stream" in self._dvctree_configs

    def open(
        self, path, mode="r", encoding="utf-8", **kwargs
    ):  # pylint: disable=arguments-differ
        if "b" in mode:
            encoding = None

        tree, dvc_tree = self._get_tree_pair(path)
        path_info = PathInfo(path)
        try:
            return tree.open(path_info, mode=mode, encoding=encoding)
        except FileNotFoundError:
            if not dvc_tree:
                raise

        return dvc_tree.open(path_info, mode=mode, encoding=encoding, **kwargs)

    def exists(
        self, path, use_dvcignore=True
    ):  # pylint: disable=arguments-differ
        tree, dvc_tree = self._get_tree_pair(path)

        if not dvc_tree:
            return tree.exists(path)

        if tree.exists(path):
            return True

        try:
            meta = dvc_tree.metadata(path)
        except FileNotFoundError:
            return False

        (out,) = meta.outs
        assert len(meta.outs) == 1
        if tree.exists(out.path_info):
            return False
        return True

    def isdir(self, path):  # pylint: disable=arguments-differ
        tree, dvc_tree = self._get_tree_pair(path)

        try:
            st = tree.stat(path)
            return stat.S_ISDIR(st.st_mode)
        except (OSError, ValueError):
            # from CPython's os.path.isdir()
            pass

        if not dvc_tree:
            return False

        try:
            meta = dvc_tree.metadata(path)
        except FileNotFoundError:
            return False

        (out,) = meta.outs
        assert len(meta.outs) == 1
        if tree.exists(out.path_info):
            return False
        return meta.isdir

    def isdvc(self, path, **kwargs):
        _, dvc_tree = self._get_tree_pair(path)
        return dvc_tree is not None and dvc_tree.isdvc(path, **kwargs)

    def isfile(self, path):  # pylint: disable=arguments-differ
        tree, dvc_tree = self._get_tree_pair(path)

        try:
            st = tree.stat(path)
            return stat.S_ISREG(st.st_mode)
        except (OSError, ValueError):
            # from CPython's os.path.isfile()
            pass

        if not dvc_tree:
            return False

        try:
            meta = dvc_tree.metadata(path)
        except FileNotFoundError:
            return False

        (out,) = meta.outs
        assert len(meta.outs) == 1
        if tree.exists(out.path_info):
            return False
        return meta.isfile

    def isexec(self, path):
        tree, dvc_tree = self._get_tree_pair(path)
        if dvc_tree and dvc_tree.exists(path):
            return dvc_tree.isexec(path)
        return tree.isexec(path)

    def stat(self, path):
        tree, _ = self._get_tree_pair(path)
        return tree.stat(path)

    def _dvc_walk(self, walk):
        try:
            root, dirs, files = next(walk)
        except StopIteration:
            return
        yield root, dirs, files
        for _ in dirs:
            yield from self._dvc_walk(walk)

    def _subrepo_walk(self, dir_path, **kwargs):
        """Walk into a new repo.

         NOTE: subrepo will only be discovered when walking if
         ignore_subrepos is set to False.
        """
        tree, dvc_tree = self._get_tree_pair(dir_path)
        tree_walk = tree.walk(
            dir_path, topdown=True, ignore_subrepos=not self._traverse_subrepos
        )
        if dvc_tree:
            dvc_walk = dvc_tree.walk(dir_path, topdown=True, **kwargs)
        else:
            dvc_walk = None
        yield from self._walk(tree_walk, dvc_walk, **kwargs)

    def _walk(self, repo_walk, dvc_walk=None, dvcfiles=False):
        assert repo_walk
        try:
            _, dvc_dirs, dvc_fnames = (
                next(dvc_walk) if dvc_walk else (None, [], [])
            )
            repo_root, repo_dirs, repo_fnames = next(repo_walk)
        except StopIteration:
            return

        # separate subdirs into shared dirs, dvc-only dirs, repo-only dirs
        dvc_set = set(dvc_dirs)
        repo_set = set(repo_dirs)
        dvc_only = list(dvc_set - repo_set)
        repo_only = list(repo_set - dvc_set)
        shared = list(dvc_set & repo_set)
        dirs = shared + dvc_only + repo_only

        # merge file lists
        files = {
            fname
            for fname in dvc_fnames + repo_fnames
            if dvcfiles or not is_valid_filename(fname)
        }

        yield repo_root, dirs, list(files)

        def is_dvc_repo(d):
            return self._is_dvc_repo(os.path.join(repo_root, d))

        # remove subrepos to prevent it from being traversed
        subrepos = set(filter(is_dvc_repo, repo_only))
        # set dir order for next recursion level - shared dirs first so that
        # next() for both generators recurses into the same shared directory
        dvc_dirs[:] = [dirname for dirname in dirs if dirname in dvc_set]
        repo_dirs[:] = lfilter(lambda d: d in (repo_set - subrepos), dirs)

        for dirname in dirs:
            if dirname in subrepos:
                dir_path = os.path.join(repo_root, dirname)
                yield from self._subrepo_walk(dir_path, dvcfiles=dvcfiles)
            elif dirname in shared:
                yield from self._walk(repo_walk, dvc_walk, dvcfiles=dvcfiles)
            elif dirname in dvc_set:
                yield from self._dvc_walk(dvc_walk)
            elif dirname in repo_set:
                yield from self._walk(repo_walk, None, dvcfiles=dvcfiles)

    def walk(
        self, top, topdown=True, onerror=None, dvcfiles=False, **kwargs
    ):  # pylint: disable=arguments-differ
        """Walk and merge both DVC and repo trees.

        Args:
            top: path to walk from
            topdown: if True, tree will be walked from top down.
            onerror: if set, onerror function will be called if an error
                occurs (by default errors are ignored).
            dvcfiles: if True, dvcfiles will be included in the files list
                for walked directories.

        Any kwargs will be passed into methods used for fetching and/or
        streaming DVC outs from remotes.
        """
        assert topdown

        if not self.exists(top):
            if onerror is not None:
                onerror(FileNotFoundError(top))
            return

        if not self.isdir(top):
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        tree, dvc_tree = self._get_tree_pair(top)
        repo_exists = tree.exists(top)
        repo_walk = tree.walk(
            top,
            topdown=topdown,
            onerror=onerror,
            ignore_subrepos=not self._traverse_subrepos,
        )

        if not dvc_tree or (repo_exists and dvc_tree.isdvc(top)):
            yield from self._walk(repo_walk, None, dvcfiles=dvcfiles)
            return

        if not repo_exists:
            yield from dvc_tree.walk(top, topdown=topdown, **kwargs)

        dvc_walk = None
        if dvc_tree.exists(top):
            dvc_walk = dvc_tree.walk(top, topdown=topdown, **kwargs)

        yield from self._walk(repo_walk, dvc_walk, dvcfiles=dvcfiles)

    def walk_files(self, top, **kwargs):  # pylint: disable=arguments-differ
        for root, _, files in self.walk(top, **kwargs):
            for fname in files:
                yield PathInfo(root) / fname

    def get_dir_hash(self, path_info, **kwargs):
        tree, dvc_tree = self._get_tree_pair(path_info)
        if tree.exists(path_info):
            return super().get_dir_hash(path_info, **kwargs)
        if not dvc_tree:
            raise FileNotFoundError
        return dvc_tree.get_dir_hash(path_info, **kwargs)

    def get_file_hash(self, path_info):
        """Return file checksum for specified path.

        If path_info is a DVC out, the pre-computed checksum for the file
        will be used. If path_info is a git file, MD5 will be computed for
        the git object.
        """
        if not self.exists(path_info):
            raise FileNotFoundError
        _, dvc_tree = self._get_tree_pair(path_info)
        if dvc_tree and dvc_tree.exists(path_info):
            try:
                return dvc_tree.get_file_hash(path_info)
            except FileNotFoundError:
                pass
        return HashInfo(self.PARAM_CHECKSUM, file_md5(path_info, self)[0])

    def copytree(self, top, dest):
        top = PathInfo(top)
        dest = PathInfo(dest)

        if not self.exists(top):
            raise FileNotFoundError

        if self.isfile(top):
            makedirs(dest.parent, exist_ok=True)
            with self.open(top, mode="rb") as fobj:
                copy_fobj_to_file(fobj, dest)
            return

        for root, _, files in self.walk(top):
            root = PathInfo(root)
            dest_dir = dest / root.relative_to(top)
            makedirs(dest_dir, exist_ok=True)
            for fname in files:
                src = root / fname
                with self.open(src, mode="rb") as fobj:
                    copy_fobj_to_file(fobj, dest_dir / fname)

    @property
    def hash_jobs(self):  # pylint: disable=invalid-overridden-method
        return self._main_repo.tree.hash_jobs

    def metadata(self, path):
        abspath = os.path.abspath(path)
        path_info = PathInfo(abspath)
        tree, dvc_tree = self._get_tree_pair(path_info)

        dvc_meta = None
        if dvc_tree:
            with suppress(FileNotFoundError):
                dvc_meta = dvc_tree.metadata(path_info)

        stat_result = None
        with suppress(FileNotFoundError):
            stat_result = tree.stat(path_info)

        if not stat_result and not dvc_meta:
            raise FileNotFoundError

        meta = dvc_meta or Metadata(
            path_info=path_info,
            repo=self._get_repo(abspath) or self._main_repo,
        )

        isdir = bool(stat_result) and stat.S_ISDIR(stat_result.st_mode)
        meta.isdir = meta.isdir or isdir

        if not dvc_meta:
            meta.is_exec = bool(stat_result) and is_exec(stat_result.st_mode)
        return meta
