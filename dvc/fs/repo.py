import logging
import os
import threading
from contextlib import suppress
from itertools import takewhile
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Type, Union

from funcy import lfilter, wrap_with

from ._callback import DEFAULT_CALLBACK
from .base import FileSystem
from .dvc import DvcFileSystem

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

RepoFactory = Union[Callable[[str], "Repo"], Type["Repo"]]


def _wrap_walk(dvc_fs, *args, **kwargs):
    for root, dnames, fnames in dvc_fs.walk(*args, **kwargs):
        yield dvc_fs.path.join(dvc_fs.repo.root_dir, root), dnames, fnames


def _ls(fs, path):
    dnames = []
    fnames = []

    with suppress(FileNotFoundError):
        for entry in fs.ls(path, detail=True):
            name = fs.path.name(entry["name"])
            if entry["type"] == "directory":
                dnames.append(name)
            else:
                fnames.append(name)

    return dnames, fnames


class RepoFileSystem(FileSystem):  # pylint:disable=abstract-method
    """DVC + git-tracked files fs.

    Args:
        repo: DVC or git repo.
        subrepos: traverse to subrepos (by default, it ignores subrepos)
        repo_factory: A function to initialize subrepo with, default is Repo.
        kwargs: Additional keyword arguments passed to the `DvcFileSystem()`.
    """

    sep = os.sep

    scheme = "local"
    PARAM_CHECKSUM = "md5"
    PARAM_REPO_URL = "repo_url"
    PARAM_REPO_ROOT = "repo_root"
    PARAM_REV = "rev"
    PARAM_CACHE_DIR = "cache_dir"
    PARAM_CACHE_TYPES = "cache_types"
    PARAM_SUBREPOS = "subrepos"

    def __init__(
        self,
        repo: Optional["Repo"] = None,
        subrepos=False,
        repo_factory: RepoFactory = None,
        **kwargs,
    ):
        super().__init__()

        from dvc.utils.collections import PathStringTrie

        if repo is None:
            repo, repo_factory = self._repo_from_fs_config(
                subrepos=subrepos, **kwargs
            )

        if not repo_factory:
            from dvc.repo import Repo

            self.repo_factory: RepoFactory = Repo
        else:
            self.repo_factory = repo_factory

        self._main_repo = repo
        self.hash_jobs = repo.fs.hash_jobs
        self.root_dir: str = repo.root_dir
        self._traverse_subrepos = subrepos

        self._subrepos_trie = PathStringTrie()
        """Keeps track of each and every path with the corresponding repo."""

        self._subrepos_trie[self.root_dir] = repo

        self._dvcfss = {}
        """Keep a dvcfs instance of each repo."""

        if hasattr(repo, "dvc_dir"):
            self._dvcfss[repo.root_dir] = DvcFileSystem(repo=repo)

    @property
    def repo_url(self):
        if self._main_repo is None:
            return None
        return self._main_repo.url

    @property
    def config(self):
        return {
            self.PARAM_REPO_URL: self.repo_url,
            self.PARAM_REPO_ROOT: self.root_dir,
            self.PARAM_REV: getattr(self._main_repo.fs, "rev", None),
            self.PARAM_CACHE_DIR: os.path.abspath(
                self._main_repo.odb.local.cache_dir
            ),
            self.PARAM_CACHE_TYPES: self._main_repo.odb.local.cache_types,
            self.PARAM_SUBREPOS: self._traverse_subrepos,
        }

    @classmethod
    def _repo_from_fs_config(
        cls, **config
    ) -> Tuple["Repo", Optional["RepoFactory"]]:
        from dvc.external_repo import erepo_factory, external_repo
        from dvc.repo import Repo

        url = config.get(cls.PARAM_REPO_URL)
        root = config.get(cls.PARAM_REPO_ROOT)
        assert url or root

        def _open(*args, **kwargs):
            # NOTE: if original repo was an erepo (and has a URL),
            # we cannot use Repo.open() since it will skip erepo
            # cache/remote setup for local URLs
            if url is None:
                return Repo.open(*args, **kwargs)
            return external_repo(*args, **kwargs)

        cache_dir = config.get(cls.PARAM_CACHE_DIR)
        cache_config = (
            {}
            if not cache_dir
            else {
                "cache": {
                    "dir": cache_dir,
                    "type": config.get(cls.PARAM_CACHE_TYPES),
                }
            }
        )
        repo_kwargs: dict = {
            "rev": config.get(cls.PARAM_REV),
            "subrepos": config.get(cls.PARAM_SUBREPOS, False),
            "uninitialized": True,
        }
        factory: Optional["RepoFactory"] = None
        if url is None:
            repo_kwargs["config"] = cache_config
        else:
            repo_kwargs["cache_dir"] = cache_dir
            factory = erepo_factory(url, cache_config)

        with _open(
            url if url else root,
            **repo_kwargs,
        ) as repo:
            return repo, factory

    def _get_repo(self, path: str) -> Optional["Repo"]:
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

        parents = (parent for parent in self.path.parents(path))
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
                repo = self.repo_factory(
                    d,
                    fs=self._main_repo.fs,
                    repo_factory=self.repo_factory,
                )
                self._dvcfss[repo.root_dir] = DvcFileSystem(repo=repo)
            self._subrepos_trie[d] = repo

    def _is_dvc_repo(self, dir_path):
        """Check if the directory is a dvc repo."""
        if not self._traverse_subrepos:
            return False

        from dvc.repo import Repo

        repo_path = os.path.join(dir_path, Repo.DVC_DIR)
        return self._main_repo.fs.isdir(repo_path)

    def _get_fs_pair(
        self, path
    ) -> Tuple[FileSystem, Optional[DvcFileSystem], str]:
        """
        Returns a pair of fss based on repo the path falls in, using prefix.
        """
        path = os.path.abspath(path)

        # fallback to the top-level repo if repo was not found
        # this can happen if the path is outside of the repo
        repo = self._get_repo(path) or self._main_repo

        dvc_fs = self._dvcfss.get(repo.root_dir)

        if path.startswith(repo.root_dir):
            dvc_path = path[len(repo.root_dir) + 1 :]

            dvc_path = dvc_path.replace("\\", "/")
        else:
            dvc_path = path

        return repo.fs, dvc_fs, dvc_path

    def open(
        self, path, mode="r", encoding="utf-8", **kwargs
    ):  # pylint: disable=arguments-renamed
        if "b" in mode:
            encoding = None

        fs, dvc_fs, dvc_path = self._get_fs_pair(path)
        try:
            return fs.open(path, mode=mode, encoding=encoding)
        except FileNotFoundError:
            if not dvc_fs:
                raise

        return dvc_fs.open(dvc_path, mode=mode, encoding=encoding, **kwargs)

    def exists(self, path) -> bool:
        path = os.path.abspath(path)

        fs, dvc_fs, dvc_path = self._get_fs_pair(path)

        if not dvc_fs:
            return fs.exists(path)

        if dvc_fs.repo.dvcignore.is_ignored(fs, path):
            return False

        if fs.exists(path):
            return True

        if not dvc_fs.exists(dvc_path):
            return False

        for p in self.path.parents(path):
            try:
                if fs.info(p)["type"] != "directory":
                    return False
            except FileNotFoundError:
                continue

        return True

    def isdir(self, path):  # pylint: disable=arguments-renamed
        path = os.path.abspath(path)

        fs, dvc_fs, dvc_path = self._get_fs_pair(path)

        if dvc_fs and dvc_fs.repo.dvcignore.is_ignored_dir(path):
            return False

        try:
            info = fs.info(path)
            return info["type"] == "directory"
        except (OSError, ValueError):
            # from CPython's os.path.isdir()
            pass

        if not dvc_fs:
            return False

        try:
            info = dvc_fs.info(dvc_path)
        except FileNotFoundError:
            return False

        for p in self.path.parents(path):
            try:
                if fs.info(p)["type"] != "directory":
                    return False
            except FileNotFoundError:
                continue

        return info["type"] == "directory"

    def isdvc(self, path, **kwargs):
        _, dvc_fs, dvc_path = self._get_fs_pair(path)
        return dvc_fs is not None and dvc_fs.isdvc(dvc_path, **kwargs)

    def isfile(self, path):  # pylint: disable=arguments-renamed
        path = os.path.abspath(path)

        fs, dvc_fs, dvc_path = self._get_fs_pair(path)

        if dvc_fs and dvc_fs.repo.dvcignore.is_ignored_file(path):
            return False

        try:
            info = fs.info(path)
            return info["type"] == "file"
        except (OSError, ValueError):
            # from CPython's os.path.isfile()
            pass

        if not dvc_fs:
            return False

        try:
            info = dvc_fs.info(dvc_path)
        except FileNotFoundError:
            return False

        for p in self.path.parents(path):
            try:
                if fs.info(p)["type"] != "directory":
                    return False
            except FileNotFoundError:
                continue

        return info["type"] == "file"

    def _subrepo_walk(self, dir_path, **kwargs):
        """Walk into a new repo.

        NOTE: subrepo will only be discovered when walking if
        ignore_subrepos is set to False.
        """
        fs, dvc_fs, dvc_path = self._get_fs_pair(dir_path)
        fs_walk = fs.walk(dir_path, topdown=True)
        yield from self._walk(fs_walk, dvc_fs, dvc_path, **kwargs)

    def _walk(self, repo_walk, dvc_fs, dvc_path, dvcfiles=False):
        from dvc.dvcfile import is_valid_filename
        from dvc.ignore import DvcIgnore

        assert repo_walk

        dvc_dirs, dvc_fnames = _ls(dvc_fs, dvc_path) if dvc_fs else ([], [])

        try:
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

        def _func(fname):
            if dvcfiles:
                return True

            return not (
                is_valid_filename(fname) or fname == DvcIgnore.DVCIGNORE_FILE
            )

        # merge file lists
        files = set(filter(_func, dvc_fnames + repo_fnames))

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
                yield from self._walk(
                    repo_walk,
                    dvc_fs,
                    dvc_fs.path.join(dvc_path, dirname),
                    dvcfiles=dvcfiles,
                )
            elif dirname in dvc_set:
                yield from _wrap_walk(
                    dvc_fs, dvc_fs.path.join(dvc_path, dirname)
                )
            elif dirname in repo_set:
                yield from self._walk(repo_walk, None, None, dvcfiles=dvcfiles)

    def walk(self, top, topdown=True, **kwargs):
        """Walk and merge both DVC and repo fss.

        Args:
            top: path to walk from
            topdown: if True, fs will be walked from top down.
            dvcfiles: if True, dvcfiles will be included in the files list
                for walked directories.

        Any kwargs will be passed into methods used for fetching and/or
        streaming DVC outs from remotes.
        """
        assert topdown

        if not self.exists(top):
            return

        if not self.isdir(top):
            return

        repo = self._get_repo(os.path.abspath(top))
        dvcfiles = kwargs.pop("dvcfiles", False)

        fs, dvc_fs, dvc_path = self._get_fs_pair(top)
        repo_exists = fs.exists(top)

        repo_walk = repo.dvcignore.walk(fs, top, topdown=topdown, **kwargs)

        if not dvc_fs or (repo_exists and dvc_fs.isdvc(dvc_path)):
            yield from self._walk(repo_walk, None, None, dvcfiles=dvcfiles)
            return

        if not repo_exists:
            yield from _wrap_walk(dvc_fs, dvc_path, topdown=topdown, **kwargs)

        yield from self._walk(repo_walk, dvc_fs, dvc_path, dvcfiles=dvcfiles)

    def find(self, path, prefix=None):
        for root, _, files in self.walk(path):
            for fname in files:
                yield self.path.join(root, fname)

    def get_file(
        self, from_info, to_file, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, dvc_fs, dvc_path = self._get_fs_pair(from_info)
        try:
            fs.get_file(  # pylint: disable=protected-access
                from_info, to_file, callback=callback, **kwargs
            )
            return
        except FileNotFoundError:
            if not dvc_fs:
                raise

        dvc_fs.get_file(  # pylint: disable=protected-access
            dvc_path, to_file, callback=callback, **kwargs
        )

    def info(self, path):
        fs, dvc_fs, dvc_path = self._get_fs_pair(path)

        try:
            dvc_info = dvc_fs.info(dvc_path)
        except FileNotFoundError:
            dvc_info = None

        try:
            from dvc.utils import is_exec

            fs_info = fs.info(path)
            fs_info["repo"] = dvc_fs.repo
            fs_info["isout"] = (
                dvc_info.get("isout", False) if dvc_info else False
            )
            fs_info["outs"] = dvc_info["outs"] if dvc_info else None
            fs_info["isdvc"] = dvc_info["isdvc"] if dvc_info else False
            fs_info["meta"] = dvc_info.get("meta") if dvc_info else None

            isexec = False
            if dvc_info:
                isexec = dvc_info["isexec"]
            elif fs_info["type"] == "file":
                isexec = is_exec(fs_info["mode"])
            fs_info["isexec"] = isexec
            return fs_info

        except FileNotFoundError:
            if not dvc_info:
                raise

            dvc_info["repo"] = dvc_fs.repo
            dvc_info["isdvc"] = True
            return dvc_info

    def checksum(self, path):
        fs, dvc_fs, dvc_path = self._get_fs_pair(path)

        try:
            return fs.checksum(path)
        except FileNotFoundError:
            return dvc_fs.checksum(dvc_path)
