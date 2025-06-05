import errno
import functools
import ntpath
import os
import posixpath
import threading
from collections import defaultdict, deque
from contextlib import ExitStack, nullcontext, suppress
from glob import has_magic
from typing import TYPE_CHECKING, Any, Callable, Optional, Union

from fsspec.spec import DEFAULT_CALLBACK, AbstractFileSystem
from funcy import wrap_with

from dvc.log import logger
from dvc.utils.threadpool import ThreadPoolExecutor
from dvc_objects.fs.base import AnyFSPath, FileSystem

from .data import DataFileSystem

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from dvc.repo import Repo
    from dvc.types import DictStrAny, StrPath

    from .callbacks import Callback

logger = logger.getChild(__name__)

RepoFactory = Union[Callable[..., "Repo"], type["Repo"]]
Key = tuple[str, ...]


def as_posix(path: str) -> str:
    return path.replace(ntpath.sep, posixpath.sep)


# NOT the same as dvc.dvcfile.is_dvc_file()!
def _is_dvc_file(fname):
    from dvc.dvcfile import is_valid_filename
    from dvc.ignore import DvcIgnore

    return is_valid_filename(fname) or fname == DvcIgnore.DVCIGNORE_FILE


def _merge_info(repo, key, fs_info, dvc_info):
    from . import utils

    ret = {"repo": repo}

    if dvc_info:
        dvc_info["isout"] = any(
            (len(out_key) <= len(key) and key[: len(out_key)] == out_key)
            for out_key in repo.index.data_keys["repo"]
        )
        dvc_info["isdvc"] = dvc_info["isout"]
        ret["dvc_info"] = dvc_info
        ret["type"] = dvc_info["type"]
        ret["size"] = dvc_info["size"]
        if not fs_info and "md5" in dvc_info:
            ret["md5"] = dvc_info["md5"]
        if not fs_info and "md5-dos2unix" in dvc_info:
            ret["md5-dos2unix"] = dvc_info["md5-dos2unix"]

    if fs_info:
        ret["type"] = fs_info["type"]
        ret["size"] = fs_info["size"]
        ret["fs_info"] = fs_info
        isexec = False
        if fs_info["type"] == "file":
            isexec = utils.is_exec(fs_info["mode"])
        ret["isexec"] = isexec

    return ret


def _get_dvc_path(dvc_fs, subkey):
    return dvc_fs.join(*subkey) if subkey else ""


class _DVCFileSystem(AbstractFileSystem):
    cachable = False
    root_marker = "/"

    def __init__(
        self,
        repo: Union["Repo", os.PathLike[str], str, None] = None,
        rev: Optional[str] = None,
        subrepos: bool = False,
        repo_factory: Optional[RepoFactory] = None,
        fo: Optional[str] = None,
        target_options: Optional[dict[str, Any]] = None,  # noqa: ARG002
        target_protocol: Optional[str] = None,  # noqa: ARG002
        config: Optional["DictStrAny"] = None,
        remote: Optional[str] = None,
        remote_config: Optional["DictStrAny"] = None,
        **kwargs,
    ) -> None:
        """DVC + git-tracked files fs.

        Args:
            repo (str | os.PathLike[str] | Repo, optional): A url or a path to a DVC/Git
                repository, or a `Repo` instance.
                Defaults to a DVC repository in the current working directory.
                Both HTTP and SSH protocols are supported for remote Git repos
                (e.g. [user@]server:project.git).
            rev (str, optional): Any Git revision such as a branch or tag name,
                a commit hash or a dvc experiment name.
                Defaults to the default branch in case of remote repositories.
                In case of a local repository, if rev is unspecified, it will
                default to the working directory.
                If the repo is not a Git repo, this option is ignored.
            subrepos (bool): traverse to subrepos.
                By default, it ignores subrepos.
            repo_factory (callable): A function to initialize subrepo with.
                The default is `Repo`.
            config (dict): Repo config to be passed into `repo_factory`.
            remote (str): Remote name to be passed into `repo_factory`.
            remote_config(dict): Remote config to be passed into `repo_factory`.

        Examples:
            - Opening a filesystem from repo in current working directory

            >>> fs = DVCFileSystem()

            - Opening a filesystem from local repository

            >>> fs = DVCFileSystem("path/to/local/repository")

            - Opening a remote repository

            >>> fs = DVCFileSystem(
            ...    "https://github.com/iterative/example-get-started",
            ...    rev="main",
            ... )
        """
        from dvc.repo import Repo

        # kwargs.get("url") is for maintaining backward compatibility
        repo = repo or fo or kwargs.get("url")
        if isinstance(repo, Repo):
            self._repo: Optional[Repo] = repo
            url = None
        else:
            self._repo = None
            url = os.fspath(repo) if repo else None

        super().__init__()
        self._repo_factory = repo_factory
        self._traverse_subrepos = subrepos
        self._repo_stack = ExitStack()
        self._repo_kwargs = {
            "url": url,
            "rev": rev,
            "subrepos": subrepos,
            "config": config,
            "remote": remote,
            "remote_config": remote_config,
        }

    def getcwd(self):
        relparts: tuple[str, ...] = ()
        assert self.repo is not None
        if self.repo.fs.isin(self.repo.fs.getcwd(), self.repo.root_dir):
            relparts = self.repo.fs.relparts(self.repo.fs.getcwd(), self.repo.root_dir)
        return self.root_marker + self.sep.join(relparts)

    @classmethod
    def join(cls, *parts: str) -> str:
        return posixpath.join(*parts)

    @classmethod
    def parts(cls, path: str) -> tuple[str, ...]:
        ret = []
        while True:
            path, part = posixpath.split(path)

            if part:
                ret.append(part)
                continue

            if path:
                ret.append(path)

            break

        ret.reverse()

        return tuple(ret)

    def normpath(self, path: str) -> str:
        return posixpath.normpath(path)

    def abspath(self, path: str) -> str:
        if not posixpath.isabs(path):
            path = self.join(self.getcwd(), path)
        return self.normpath(path)

    def relpath(self, path: str, start: Optional[str] = None) -> str:
        if start is None:
            start = "."
        return posixpath.relpath(self.abspath(path), start=self.abspath(start))

    def relparts(self, path: str, start: Optional[str] = None) -> tuple[str, ...]:
        return self.parts(self.relpath(path, start=start))

    @functools.cached_property
    def repo(self):
        if self._repo:
            return self._repo

        repo = self._make_repo(**self._repo_kwargs)

        self._repo_stack.enter_context(repo)
        self._repo = repo
        return repo

    @functools.cached_property
    def repo_factory(self):
        if self._repo_factory:
            return self._repo_factory

        if self._repo:
            from dvc.repo import Repo

            return Repo

        return self.repo._fs_conf["repo_factory"]

    @functools.cached_property
    def fsid(self) -> str:
        from fsspec.utils import tokenize

        from dvc.scm import NoSCM

        return "dvcfs_" + tokenize(
            self.repo.url or self.repo.root_dir,
            self.repo.get_rev() if not isinstance(self.repo.scm, NoSCM) else None,
        )

    def _get_key(self, path: "StrPath") -> Key:
        path = os.fspath(path)
        parts = self.repo.fs.relparts(path, self.repo.root_dir)
        if parts == (os.curdir,):
            return ()
        return parts

    @functools.cached_property
    def _subrepos_trie(self):
        """Keeps track of each and every path with the corresponding repo."""

        from pygtrie import Trie

        trie = Trie()
        key = self._get_key(self.repo.root_dir)
        trie[key] = self.repo
        return trie

    def _get_key_from_relative(self, path) -> Key:
        path = self._strip_protocol(path)
        parts = self.relparts(path, self.root_marker)
        if parts and parts[0] == os.curdir:
            return parts[1:]
        return parts

    def _from_key(self, parts: Key) -> str:
        return self.repo.fs.join(self.repo.root_dir, *parts)

    @functools.cached_property
    def _datafss(self):
        """Keep a datafs instance of each repo."""

        datafss = {}

        if hasattr(self.repo, "dvc_dir"):
            key = self._get_key(self.repo.root_dir)
            datafss[key] = DataFileSystem(index=self.repo.index.data["repo"])

        return datafss

    @property
    def repo_url(self):
        return self.repo.url

    @classmethod
    def _make_repo(cls, **kwargs) -> "Repo":
        from dvc.repo import Repo

        with Repo.open(uninitialized=True, **kwargs) as repo:
            return repo

    def _get_repo(self, key: Key) -> "Repo":
        """Returns repo that the path falls in, using prefix.

        If the path is already tracked/collected, it just returns the repo.

        Otherwise, it collects the repos that might be in the path's parents
        and then returns the appropriate one.
        """
        repo = self._subrepos_trie.get(key)
        if repo:
            return repo

        prefix_key, repo = self._subrepos_trie.longest_prefix(key)
        dir_keys = (key[:i] for i in range(len(prefix_key) + 1, len(key) + 1))
        self._update(dir_keys, starting_repo=repo)
        return self._subrepos_trie.get(key) or self.repo

    @wrap_with(threading.Lock())
    def _update(self, dir_keys, starting_repo):
        """Checks for subrepo in directories and updates them."""
        repo = starting_repo
        for key in dir_keys:
            d = self._from_key(key)
            if self._is_dvc_repo(d):
                repo = self.repo_factory(
                    d,
                    fs=self.repo.fs,
                    scm=self.repo.scm,
                    repo_factory=self.repo_factory,
                )
                self._repo_stack.enter_context(repo)
                self._datafss[key] = DataFileSystem(index=repo.index.data["repo"])
            self._subrepos_trie[key] = repo

    def _is_dvc_repo(self, dir_path):
        """Check if the directory is a dvc repo."""
        if not self._traverse_subrepos:
            return False

        from dvc.repo import Repo

        repo_path = self.repo.fs.join(dir_path, Repo.DVC_DIR)
        return self.repo.fs.isdir(repo_path)

    def _get_subrepo_info(
        self, key: Key
    ) -> tuple["Repo", Optional[DataFileSystem], Key]:
        """
        Returns information about the subrepo the key is part of.
        """
        repo = self._get_repo(key)
        repo_key: Key
        if repo is self.repo:
            repo_key = ()
            subkey = key
        else:
            repo_key = self._get_key(repo.root_dir)
            subkey = key[len(repo_key) :]

        dvc_fs = self._datafss.get(repo_key)
        return repo, dvc_fs, subkey

    def _open(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise OSError(errno.EROFS, os.strerror(errno.EROFS))

        key = self._get_key_from_relative(path)
        fs_path = self._from_key(key)
        try:
            return self.repo.fs.open(fs_path, mode=mode)
        except FileNotFoundError:
            _, dvc_fs, subkey = self._get_subrepo_info(key)
            if not dvc_fs:
                raise

        dvc_path = _get_dvc_path(dvc_fs, subkey)
        return dvc_fs.open(dvc_path, mode=mode, cache=kwargs.get("cache", False))

    def isdvc(self, path, **kwargs) -> bool:
        """Is this entry dvc-tracked?"""
        try:
            return self.info(path).get("dvc_info", {}).get("isout", False)
        except FileNotFoundError:
            return False

    def ls(self, path, detail=True, dvc_only=False, **kwargs):  # noqa: C901, PLR0912
        key = self._get_key_from_relative(path)
        repo, dvc_fs, subkey = self._get_subrepo_info(key)

        dvc_infos = {}
        dvc_info = {}
        if dvc_fs:
            dvc_path = _get_dvc_path(dvc_fs, subkey)
            with suppress(FileNotFoundError):
                dvc_info = dvc_fs.info(dvc_path)
                if dvc_info["type"] == "file":
                    dvc_infos[""] = dvc_info
                else:
                    for info in dvc_fs.ls(dvc_path, detail=True):
                        dvc_infos[dvc_fs.name(info["name"])] = info

        fs_infos = {}
        fs_info = {}
        ignore_subrepos = kwargs.get("ignore_subrepos", True)
        if not dvc_only:
            fs = self.repo.fs
            fs_path = self._from_key(key)
            try:
                fs_info = fs.info(fs_path)
                if fs_info["type"] == "file":
                    fs_infos[""] = fs_info
                else:
                    for info in repo.dvcignore.ls(
                        fs, fs_path, detail=True, ignore_subrepos=ignore_subrepos
                    ):
                        fs_infos[fs.name(info["name"])] = info
            except (FileNotFoundError, NotADirectoryError):
                pass

        if not (fs_info or dvc_info):
            # broken symlink or TreeError
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        if fs_info and dvc_info and dvc_info["type"] != fs_info["type"]:
            dvc_infos.clear()  # invalidate dvc_info if file type differs

        dvcfiles = kwargs.get("dvcfiles", False)

        infos = []
        paths = []
        names = set(dvc_infos.keys()) | set(fs_infos.keys())

        for name in names:
            if not dvcfiles and _is_dvc_file(name):
                continue

            entry_path = self.join(path, name) if name else path
            info = _merge_info(
                repo, (*subkey, name), fs_infos.get(name), dvc_infos.get(name)
            )
            info["name"] = entry_path
            infos.append(info)
            paths.append(entry_path)

        if not detail:
            return paths

        return infos

    def info(self, path, **kwargs):
        key = self._get_key_from_relative(path)
        ignore_subrepos = kwargs.get("ignore_subrepos", True)
        return self._info(key, path, ignore_subrepos=ignore_subrepos)

    def _info(  # noqa: C901
        self, key, path, ignore_subrepos=True, check_ignored=True
    ):
        repo, dvc_fs, subkey = self._get_subrepo_info(key)

        dvc_info = None
        if dvc_fs:
            try:
                dvc_info = dvc_fs.fs.index.info(subkey)
                dvc_path = _get_dvc_path(dvc_fs, subkey)
                dvc_info["name"] = dvc_path
            except KeyError:
                pass

        fs_info = None
        fs = self.repo.fs
        fs_path = self._from_key(key)
        try:
            fs_info = fs.info(fs_path)
            if check_ignored and repo.dvcignore.is_ignored(
                fs, fs_path, ignore_subrepos=ignore_subrepos
            ):
                fs_info = None
        except (FileNotFoundError, NotADirectoryError):
            if not dvc_info:
                raise

        # NOTE: if some parent in fs_path turns out to be a file, it means
        # that the whole repofs branch doesn't exist.
        if dvc_info and not fs_info:
            for parent in fs.parents(fs_path):
                try:
                    if fs.info(parent)["type"] != "directory":
                        dvc_info = None
                        break
                except FileNotFoundError:
                    continue

        if not dvc_info and not fs_info:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        info = _merge_info(repo, subkey, fs_info, dvc_info)
        info["name"] = path
        return info

    def get(
        self,
        rpath,
        lpath,
        recursive=False,
        callback=DEFAULT_CALLBACK,
        maxdepth=None,
        batch_size=None,
        **kwargs,
    ):
        self._get(
            rpath,
            lpath,
            recursive=recursive,
            callback=callback,
            maxdepth=maxdepth,
            batch_size=batch_size,
            **kwargs,
        )

    def _get(  # noqa: C901, PLR0912, PLR0915
        self,
        rpath,
        lpath,
        recursive=False,
        callback=DEFAULT_CALLBACK,
        maxdepth=None,
        batch_size=None,
        **kwargs,
    ) -> list[tuple[str, str, Optional[dict]]]:
        if (
            isinstance(rpath, list)
            or isinstance(lpath, list)
            or has_magic(rpath)
            or not self.exists(rpath)
            or not recursive
        ):
            super().get(
                rpath,
                lpath,
                recursive=recursive,
                callback=callback,
                maxdepth=maxdepth,
                **kwargs,
            )
            return []

        if os.path.isdir(lpath) or lpath.endswith(os.path.sep):
            lpath = self.join(lpath, os.path.basename(rpath))

        if self.isfile(rpath):
            with callback.branched(rpath, lpath) as child:
                self.get_file(rpath, lpath, callback=child, **kwargs)
                return [(rpath, lpath, None)]

        result: list[tuple[str, str, Optional[dict]]] = []
        _dirs: list[str] = []
        _files: dict[FileSystem, list[tuple[str, str, Optional[dict]]]]
        _files = defaultdict(list)

        for root, dirs, files in self.walk(rpath, maxdepth=maxdepth, detail=True):
            if files:
                callback.set_size((callback.size or 0) + len(files))

            parts = self.relparts(root, rpath)
            if parts in ((os.curdir,), ("",)):
                parts = ()
            dest_root = os.path.join(lpath, *parts)
            if not maxdepth or len(parts) < maxdepth - 1:
                _dirs.extend(f"{dest_root}{os.path.sep}{d}" for d in dirs)

            key = self._get_key_from_relative(root)
            _, dvc_fs, _ = self._get_subrepo_info(key)

            for name, info in files.items():
                dvc_info = info.get("dvc_info")
                fs_info = info.get("fs_info")
                if dvc_fs and dvc_info and not fs_info:
                    fs = dvc_fs
                    fs_path = dvc_info["name"]
                else:
                    fs = self.repo.fs
                    fs_path = fs_info["name"]

                src_path = f"{root}{self.sep}{name}"
                dest_path = f"{dest_root}{os.path.sep}{name}"
                _files[fs].append((fs_path, dest_path, dvc_info))
                result.append((src_path, dest_path, info))

        os.makedirs(lpath, exist_ok=True)
        for d in _dirs:
            os.makedirs(d, exist_ok=True)

        def get_file(arg: tuple[FileSystem, tuple[str, str, Optional[dict]]]):
            fs, (src, dest, info) = arg
            kw = kwargs
            if isinstance(fs, DataFileSystem):
                kw = kw | {"info": info}
            with callback.branched(src, dest) as child:
                fs.get_file(src, dest, callback=child, **kw)

        if batch_size == 1:
            ctx: AbstractContextManager = nullcontext()
            map_fn: Callable = map
        else:
            ctx = ThreadPoolExecutor(max_workers=batch_size)
            map_fn = ctx.imap_unordered

        with ctx:
            it = ((fs, f) for fs, files in _files.items() for f in files)
            deque(callback.wrap(map_fn(get_file, it)), maxlen=0)
        return result

    def get_file(self, rpath, lpath, **kwargs):
        dvc_info = kwargs.pop("info", {}).pop("dvc_info", None)
        key = self._get_key_from_relative(rpath)
        fs_path = self._from_key(key)
        dirpath = os.path.dirname(lpath)
        if dirpath:
            # makedirs raises error if the string is empty
            os.makedirs(dirpath, exist_ok=True)

        try:
            return self.repo.fs.get_file(fs_path, lpath, **kwargs)
        except FileNotFoundError:
            _, dvc_fs, subkey = self._get_subrepo_info(key)
            if not dvc_fs:
                raise

        dvc_path = _get_dvc_path(dvc_fs, subkey)
        return dvc_fs.get_file(dvc_path, lpath, info=dvc_info, **kwargs)

    def du(self, path, total=True, maxdepth=None, withdirs=False, **kwargs):
        if maxdepth is not None:
            raise NotImplementedError

        sizes = {}
        dus = {}
        todo = deque([self.info(path)])
        while todo:
            info = todo.popleft()
            isdir = info["type"] == "directory"
            size = info["size"] or 0
            name = info["name"]

            if not isdir:
                sizes[name] = size
                continue

            dvc_info = info.get("dvc_info") or {}
            fs_info = info.get("fs_info")
            entry = dvc_info.get("entry")
            if (
                dvc_info
                and not fs_info
                and entry is not None
                and entry.size is not None
            ):
                dus[name] = entry.size
                continue

            if withdirs:
                sizes[name] = size

            todo.extend(self.ls(info["name"], detail=True))

        if total:
            return sum(sizes.values()) + sum(dus.values())

        return sizes

    def close(self):
        self._repo_stack.close()


class DVCFileSystem(FileSystem):
    protocol = "local"
    PARAM_CHECKSUM = "md5"

    def _prepare_credentials(self, **config) -> dict[str, Any]:
        return config

    @functools.cached_property
    def fs(self) -> "_DVCFileSystem":
        return _DVCFileSystem(**self.fs_args)

    @property
    def immutable(self):
        from dvc.scm import NoSCM

        if isinstance(self.fs.repo.scm, NoSCM):
            return False

        return self.fs._repo_kwargs.get("rev") == self.fs.repo.get_rev()

    def getcwd(self):
        return self.fs.getcwd()

    def _get(
        self,
        from_info: Union[AnyFSPath, list[AnyFSPath]],
        to_info: Union[AnyFSPath, list[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        recursive: bool = False,
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> list[tuple[str, str, Optional[dict]]]:
        # FileSystem.get is non-recursive by default if arguments are lists
        # otherwise, it's recursive.
        recursive = not (isinstance(from_info, list) and isinstance(to_info, list))
        return self.fs._get(
            from_info,
            to_info,
            callback=callback,
            recursive=recursive,
            batch_size=batch_size,
            **kwargs,
        )

    def get(
        self,
        from_info: Union[AnyFSPath, list[AnyFSPath]],
        to_info: Union[AnyFSPath, list[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        recursive: bool = False,
        batch_size: Optional[int] = None,
        **kwargs,
    ) -> None:
        self._get(
            from_info,
            to_info,
            callback=callback,
            batch_size=batch_size,
            recursive=recursive,
            **kwargs,
        )

    @property
    def fsid(self) -> str:
        return self.fs.fsid

    def isdvc(self, path, **kwargs) -> bool:
        return self.fs.isdvc(path, **kwargs)

    @property
    def repo(self) -> "Repo":
        return self.fs.repo

    @property
    def repo_url(self) -> str:
        return self.fs.repo_url

    def from_os_path(self, path: str) -> str:
        if os.path.isabs(path) or (
            os.name == "nt" and posixpath.isabs(path) and ntpath.sep not in path
        ):
            path = os.path.relpath(path, self.repo.root_dir)
        return as_posix(path)

    def close(self):
        if "fs" in self.__dict__:
            self.fs.close()
