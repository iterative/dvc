import logging
import os
import threading
from contextlib import suppress
from itertools import takewhile
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Type, Union

from funcy import wrap_with

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

    for entry in fs.ls(path, detail=True):
        name = fs.path.name(entry["name"])
        if entry["type"] == "directory":
            dnames.append(name)
        else:
            fnames.append(name)

    return dnames, fnames


def _merge_info(repo, fs_info, dvc_info):
    from dvc.utils import is_exec

    if not fs_info:
        dvc_info["repo"] = repo
        dvc_info["isdvc"] = True
        return dvc_info

    fs_info["repo"] = repo
    fs_info["isout"] = dvc_info.get("isout", False) if dvc_info else False
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

        return repo.fs, path, dvc_fs, dvc_path

    def open(
        self, path, mode="r", encoding="utf-8", **kwargs
    ):  # pylint: disable=arguments-renamed
        if "b" in mode:
            encoding = None

        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)
        try:
            return fs.open(fs_path, mode=mode, encoding=encoding)
        except FileNotFoundError:
            if not dvc_fs:
                raise

        return dvc_fs.open(dvc_path, mode=mode, encoding=encoding, **kwargs)

    def exists(self, path, **kwargs) -> bool:
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        if not dvc_fs:
            return fs.exists(fs_path)

        if dvc_fs.repo.dvcignore.is_ignored(fs, path, **kwargs):
            return False

        if fs.exists(path):
            return True

        if not dvc_fs.exists(dvc_path):
            return False

        for p in self.path.parents(fs_path):
            try:
                if fs.info(p)["type"] != "directory":
                    return False
            except FileNotFoundError:
                continue

        return True

    def isdir(self, path, **kwargs):  # pylint: disable=arguments-renamed
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        if dvc_fs and dvc_fs.repo.dvcignore.is_ignored_dir(fs_path, **kwargs):
            return False

        try:
            info = fs.info(fs_path)
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

        for p in fs.path.parents(fs_path):
            try:
                if fs.info(p)["type"] != "directory":
                    return False
            except FileNotFoundError:
                continue

        return info["type"] == "directory"

    def isdvc(self, path, **kwargs):
        _, _, dvc_fs, dvc_path = self._get_fs_pair(path)
        return dvc_fs is not None and dvc_fs.isdvc(dvc_path, **kwargs)

    def isfile(self, path, **kwargs):  # pylint: disable=arguments-renamed
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        if dvc_fs and dvc_fs.repo.dvcignore.is_ignored_file(fs_path, **kwargs):
            return False

        try:
            info = fs.info(fs_path)
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

        for p in fs.path.parents(fs_path):
            try:
                if fs.info(p)["type"] != "directory":
                    return False
            except FileNotFoundError:
                continue

        return info["type"] == "file"

    def ls(self, path, detail=True, **kwargs):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        repo = self._get_repo(os.path.abspath(path))
        dvcignore = repo.dvcignore
        ignore_subrepos = kwargs.get("ignore_subrepos", True)

        dvc_entries = []
        if dvc_fs:
            with suppress(FileNotFoundError):
                dvc_entries = dvc_fs.ls(dvc_path, detail=True)

        fs_entries = []
        try:
            fs_entries = dvcignore.ls(
                fs, fs_path, detail=True, ignore_subrepos=ignore_subrepos
            )
        except (FileNotFoundError, NotADirectoryError):
            pass

        def _to_dict(fs, entries):
            return {fs.path.name(entry["name"]): entry for entry in entries}

        dvc_dict = _to_dict(dvc_fs, dvc_entries)
        fs_dict = _to_dict(fs, fs_entries)
        overlay = {**dvc_dict, **fs_dict}

        dvcfiles = kwargs.get("dvcfiles", False)

        def _func(fname):
            from dvc.dvcfile import is_valid_filename
            from dvc.ignore import DvcIgnore

            if dvcfiles:
                return True

            return not (
                is_valid_filename(fname) or fname == DvcIgnore.DVCIGNORE_FILE
            )

        names = filter(_func, overlay.keys())

        if not detail:
            return list(names)

        return [
            _merge_info(dvc_fs.repo, fs_dict.get(name), dvc_dict.get(name))
            for name in names
        ]

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
        dirs = []
        nondirs = []

        ignore_subrepos = kwargs.get("ignore_subrepos", True)

        if not self.exists(top, ignore_subrepos=ignore_subrepos):
            return

        if not self.isdir(top, ignore_subrepos=ignore_subrepos):
            return

        for entry in self.ls(top, **kwargs):
            name = self.path.name(entry["name"])
            if entry["type"] == "directory":
                dirs.append(name)
            else:
                nondirs.append(name)

        if topdown:
            yield top, dirs, nondirs

        for dname in dirs:
            yield from self.walk(
                self.path.join(top, dname), topdown=topdown, **kwargs
            )

        if not topdown:
            yield top, dirs, nondirs

    def find(self, path, prefix=None):
        for root, _, files in self.walk(path):
            for fname in files:
                yield self.path.join(root, fname)

    def get_file(
        self, from_info, to_file, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(from_info)
        try:
            fs.get_file(  # pylint: disable=protected-access
                fs_path, to_file, callback=callback, **kwargs
            )
            return
        except FileNotFoundError:
            if not dvc_fs:
                raise

        dvc_fs.get_file(  # pylint: disable=protected-access
            dvc_path, to_file, callback=callback, **kwargs
        )

    def info(self, path):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        try:
            dvc_info = dvc_fs.info(dvc_path)
        except FileNotFoundError:
            dvc_info = None

        try:
            fs_info = fs.info(fs_path)
        except FileNotFoundError:
            if not dvc_info:
                raise
            fs_info = None

        return _merge_info(dvc_fs.repo, fs_info, dvc_info)

    def checksum(self, path):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        try:
            return fs.checksum(fs_path)
        except FileNotFoundError:
            return dvc_fs.checksum(dvc_path)
