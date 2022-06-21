import logging
import ntpath
import os
import posixpath
import threading
from contextlib import suppress
from itertools import takewhile
from typing import TYPE_CHECKING, Callable, Optional, Tuple, Type, Union

from fsspec.spec import AbstractFileSystem
from funcy import cached_property, wrap_prop, wrap_with

from dvc_objects.fs.base import FileSystem
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK
from dvc_objects.fs.path import Path

from .data import DataFileSystem

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

RepoFactory = Union[Callable[[str], "Repo"], Type["Repo"]]


def as_posix(path: str) -> str:
    return path.replace(ntpath.sep, posixpath.sep)


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
    from . import utils

    ret = {"repo": repo}

    if dvc_info:
        ret["dvc_info"] = dvc_info
        ret["type"] = dvc_info["type"]
        ret["size"] = dvc_info["size"]
        if not fs_info and "md5" in dvc_info:
            ret["md5"] = dvc_info["md5"]

    if fs_info:
        ret["type"] = fs_info["type"]
        ret["size"] = fs_info["size"]
        isexec = False
        if fs_info["type"] == "file":
            isexec = utils.is_exec(fs_info["mode"])
        ret["isexec"] = isexec

    return ret


class _DvcFileSystem(AbstractFileSystem):  # pylint:disable=abstract-method
    """DVC + git-tracked files fs.

    Args:
        repo: DVC or git repo.
        subrepos: traverse to subrepos (by default, it ignores subrepos)
        repo_factory: A function to initialize subrepo with, default is Repo.
        kwargs: Additional keyword arguments passed to the `DataFileSystem()`.
    """

    root_marker = "/"

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

        from pygtrie import Trie

        if repo is None:
            repo, repo_factory = self._repo_from_fs_config(
                subrepos=subrepos, **kwargs
            )

        if not repo_factory:
            from dvc.repo import Repo

            self.repo_factory: RepoFactory = Repo
        else:
            self.repo_factory = repo_factory

        def _getcwd():
            relparts = ()
            if repo.fs.path.isin(repo.fs.path.getcwd(), repo.root_dir):
                relparts = repo.fs.path.relparts(
                    repo.fs.path.getcwd(), repo.root_dir
                )
            return self.root_marker + self.sep.join(relparts)

        self.path = Path(self.sep, getcwd=_getcwd)
        self.repo = repo
        self.hash_jobs = repo.fs.hash_jobs
        self._traverse_subrepos = subrepos

        self._subrepos_trie = Trie()
        """Keeps track of each and every path with the corresponding repo."""

        key = self._get_key(self.repo.root_dir)
        self._subrepos_trie[key] = repo

        self._datafss = {}
        """Keep a datafs instance of each repo."""

        if hasattr(repo, "dvc_dir"):
            self._datafss[key] = DataFileSystem(repo=repo)

    def _get_key(self, path):
        parts = self.repo.fs.path.relparts(path, self.repo.root_dir)
        if parts == (".",):
            parts = ()
        return parts

    @property
    def repo_url(self):
        if self.repo is None:
            return None
        return self.repo.url

    @property
    def config(self):
        return {
            self.PARAM_REPO_URL: self.repo_url,
            self.PARAM_REPO_ROOT: self.repo.root_dir,
            self.PARAM_REV: getattr(self.repo.fs, "rev", None),
            self.PARAM_CACHE_DIR: os.path.abspath(
                self.repo.odb.local.cache_dir
            ),
            self.PARAM_CACHE_TYPES: self.repo.odb.local.cache_types,
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
            factory = erepo_factory(url, root, cache_config)

        with _open(
            url if url else root,
            **repo_kwargs,
        ) as repo:
            return repo, factory

    def _get_repo(self, path: str) -> "Repo":
        """Returns repo that the path falls in, using prefix.

        If the path is already tracked/collected, it just returns the repo.

        Otherwise, it collects the repos that might be in the path's parents
        and then returns the appropriate one.
        """
        if not self.repo.fs.path.isin_or_eq(path, self.repo.root_dir):
            # outside of repo
            return self.repo

        key = self._get_key(path)
        repo = self._subrepos_trie.get(key)
        if repo:
            return repo

        prefix_key, repo = self._subrepos_trie.longest_prefix(key)
        prefix = self.repo.fs.path.join(
            self.repo.root_dir,
            *prefix_key,  # pylint: disable=not-an-iterable
        )

        parents = (parent for parent in self.repo.fs.path.parents(path))
        dirs = [path] + list(takewhile(lambda p: p != prefix, parents))
        dirs.reverse()
        self._update(dirs, starting_repo=repo)
        return self._subrepos_trie.get(key) or self.repo

    @wrap_with(threading.Lock())
    def _update(self, dirs, starting_repo):
        """Checks for subrepo in directories and updates them."""
        repo = starting_repo
        for d in dirs:
            key = self._get_key(d)
            if self._is_dvc_repo(d):
                repo = self.repo_factory(
                    d,
                    fs=self.repo.fs,
                    scm=self.repo.scm,
                    repo_factory=self.repo_factory,
                )
                self._datafss[key] = DataFileSystem(repo=repo)
            self._subrepos_trie[key] = repo

    def _is_dvc_repo(self, dir_path):
        """Check if the directory is a dvc repo."""
        if not self._traverse_subrepos:
            return False

        from dvc.repo import Repo

        repo_path = self.repo.fs.path.join(dir_path, Repo.DVC_DIR)
        return self.repo.fs.isdir(repo_path)

    def _get_fs_pair(
        self, path
    ) -> Tuple[
        Optional[FileSystem],
        Optional[str],
        Optional[DataFileSystem],
        Optional[str],
    ]:
        """
        Returns a pair of fss based on repo the path falls in, using prefix.
        """
        parts = self.path.relparts(path, self.root_marker)
        if parts and parts[0] == os.curdir:
            parts = parts[1:]

        fs_path = self.repo.fs.path.join(self.repo.root_dir, *parts)
        repo = self._get_repo(fs_path)
        fs = repo.fs

        repo_parts = fs.path.relparts(repo.root_dir, self.repo.root_dir)
        if repo_parts[0] == os.curdir:
            repo_parts = repo_parts[1:]

        dvc_parts = parts[len(repo_parts) :]
        if dvc_parts and dvc_parts[0] == os.curdir:
            dvc_parts = dvc_parts[1:]

        key = self._get_key(repo.root_dir)
        dvc_fs = self._datafss.get(key)
        if dvc_fs:
            dvc_path = dvc_fs.path.join(*dvc_parts) if dvc_parts else ""
        else:
            dvc_path = None

        return fs, fs_path, dvc_fs, dvc_path

    def open(
        self, path, mode="r", encoding="utf-8", **kwargs
    ):  # pylint: disable=arguments-renamed, arguments-differ
        if "b" in mode:
            encoding = None

        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)
        try:
            return fs.open(fs_path, mode=mode, encoding=encoding)
        except FileNotFoundError:
            if not dvc_fs:
                raise

        return dvc_fs.open(dvc_path, mode=mode, encoding=encoding, **kwargs)

    def isdvc(self, path, **kwargs):
        _, _, dvc_fs, dvc_path = self._get_fs_pair(path)
        return dvc_fs is not None and dvc_fs.isdvc(dvc_path, **kwargs)

    def ls(  # pylint: disable=arguments-differ
        self, path, detail=True, dvc_only=False, **kwargs
    ):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        repo = dvc_fs.repo if dvc_fs else self.repo
        dvcignore = repo.dvcignore
        ignore_subrepos = kwargs.get("ignore_subrepos", True)

        names = set()
        if dvc_fs:
            with suppress(FileNotFoundError):
                for entry in dvc_fs.ls(dvc_path, detail=False):
                    names.add(dvc_fs.path.name(entry))

        if not dvc_only and fs:
            try:
                for entry in dvcignore.ls(
                    fs, fs_path, detail=False, ignore_subrepos=ignore_subrepos
                ):
                    names.add(fs.path.name(entry))
            except (FileNotFoundError, NotADirectoryError):
                pass

        dvcfiles = kwargs.get("dvcfiles", False)

        def _func(fname):
            from dvc.dvcfile import is_valid_filename
            from dvc.ignore import DvcIgnore

            if dvcfiles:
                return True

            return not (
                is_valid_filename(fname) or fname == DvcIgnore.DVCIGNORE_FILE
            )

        names = filter(_func, names)

        infos = []
        paths = []
        for name in names:
            entry_path = self.path.join(path, name)
            try:
                info = self.info(entry_path, ignore_subrepos=ignore_subrepos)
            except FileNotFoundError:
                continue
            infos.append(info)
            paths.append(entry_path)

        if not detail:
            return paths

        return infos

    def get_file(  # pylint: disable=arguments-differ
        self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(rpath)

        if fs:
            try:
                fs.get_file(fs_path, lpath, callback=callback, **kwargs)
                return
            except FileNotFoundError:
                if not dvc_fs:
                    raise
        dvc_fs.get_file(dvc_path, lpath, callback=callback, **kwargs)

    def info(self, path, **kwargs):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        repo = dvc_fs.repo if dvc_fs else self.repo
        dvcignore = repo.dvcignore
        ignore_subrepos = kwargs.get("ignore_subrepos", True)

        dvc_info = None
        if dvc_fs:
            try:
                dvc_info = dvc_fs.info(dvc_path)
            except FileNotFoundError:
                pass

        fs_info = None
        if fs:
            try:
                fs_info = fs.info(fs_path)
                if dvcignore.is_ignored(
                    fs, fs_path, ignore_subrepos=ignore_subrepos
                ):
                    fs_info = None
            except (FileNotFoundError, NotADirectoryError):
                if not dvc_info:
                    raise

        # NOTE: if some parent in fs_path turns out to be a file, it means
        # that the whole repofs branch doesn't exist.
        if fs and not fs_info and dvc_info:
            for parent in fs.path.parents(fs_path):
                try:
                    if fs.info(parent)["type"] != "directory":
                        dvc_info = None
                        break
                except FileNotFoundError:
                    continue

        if not dvc_info and not fs_info:
            raise FileNotFoundError

        info = _merge_info(repo, fs_info, dvc_info)
        info["name"] = path
        return info

    def checksum(self, path):
        fs, fs_path, dvc_fs, dvc_path = self._get_fs_pair(path)

        try:
            return fs.checksum(fs_path)
        except FileNotFoundError:
            return dvc_fs.checksum(dvc_path)


class DvcFileSystem(FileSystem):
    protocol = "local"
    PARAM_CHECKSUM = "md5"

    def _prepare_credentials(self, **config):
        return config

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        return _DvcFileSystem(**self.fs_args)

    def isdvc(self, path, **kwargs):
        return self.fs.isdvc(path, **kwargs)

    @property
    def path(self):  # pylint: disable=invalid-overridden-method
        return self.fs.path

    @property
    def repo(self):
        return self.fs.repo

    @property
    def repo_url(self):
        return self.fs.repo_url

    @property
    def config(self):
        return self.fs.config

    def from_os_path(self, path):
        if os.path.isabs(path):
            path = os.path.relpath(path, self.repo.root_dir)

        return as_posix(path)
