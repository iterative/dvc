import logging
import threading
import typing

from fsspec import AbstractFileSystem
from funcy import cached_property, wrap_prop

from dvc_objects.fs.base import FileSystem
from dvc_objects.fs.callbacks import DEFAULT_CALLBACK

if typing.TYPE_CHECKING:
    from dvc.types import AnyPath

logger = logging.getLogger(__name__)


class _DataFileSystem(AbstractFileSystem):  # pylint:disable=abstract-method
    """DVC repo fs.

    Args:
        repo: DVC repo.
    """

    root_marker = "/"

    def __init__(self, workspace=None, **kwargs):
        super().__init__(**kwargs)
        self.repo = kwargs["repo"]
        self.workspace = workspace or "repo"

    @cached_property
    def path(self):
        from . import Path

        def _getcwd():
            return self.root_marker

        return Path(self.sep, getcwd=_getcwd)

    @property
    def config(self):
        raise NotImplementedError

    def _get_key(self, path):
        if self.workspace != "repo":
            from . import get_cloud_fs

            cls, kwargs, fs_path = get_cloud_fs(None, url=path)
            fs = cls(**kwargs)
            return (self.workspace, *fs.path.parts(fs_path))

        path = self.path.abspath(path)
        if path == self.root_marker:
            return (self.workspace,)

        key = self.path.relparts(path, self.root_marker)
        if key == (".") or key == (""):
            key = ()

        return (self.workspace, *key)

    def _get_fs_path(self, path: "AnyPath", remote=None):
        from dvc.config import NoRemoteError

        info = self.info(path)
        if info["type"] == "directory":
            raise IsADirectoryError

        value = info.get("md5")
        if not value:
            raise FileNotFoundError

        cache_path = self.repo.odb.local.oid_to_path(value)

        if self.repo.odb.local.fs.exists(cache_path):
            return self.repo.odb.local.fs, cache_path

        try:
            remote_odb = self.repo.cloud.get_remote_odb(remote)
        except NoRemoteError as exc:
            raise FileNotFoundError from exc
        remote_fs_path = remote_odb.oid_to_path(value)
        return remote_odb.fs, remote_fs_path

    def open(  # type: ignore
        self, path: str, mode="r", encoding=None, **kwargs
    ):  # pylint: disable=arguments-renamed, arguments-differ
        fs, fspath = self._get_fs_path(path, **kwargs)
        return fs.open(fspath, mode=mode, encoding=encoding)

    def ls(self, path, detail=True, **kwargs):
        info = self.info(path)
        if info["type"] != "directory":
            return [info] if detail else [path]

        root_key = self._get_key(path)
        try:
            entries = [
                self.sep.join((path, name)) if path else name
                for name in self.repo.index.tree.ls(prefix=root_key)
            ]
        except KeyError as exc:
            raise FileNotFoundError from exc

        if not detail:
            return entries

        return [self.info(epath) for epath in entries]

    def isdvc(self, path, recursive=False, strict=True):
        try:
            info = self.info(path)
        except FileNotFoundError:
            return False

        recurse = recursive or not strict
        return bool(info.get("outs") if recurse else info.get("isout"))

    def info(self, path, **kwargs):
        from dvc_data.hashfile.meta import Meta

        key = self._get_key(path)

        try:
            outs = list(self.repo.index.tree.iteritems(key))  # noqa: B301
        except KeyError as exc:
            raise FileNotFoundError from exc

        ret = {
            "type": "file",
            "size": 0,
            "isexec": False,
            "isdvc": False,
            "outs": outs,
            "name": path,
        }

        if len(outs) > 1 and outs[0][0] != key:
            shortest = self.repo.index.tree.shortest_prefix(key)
            if shortest:
                assert shortest[1][1].isdir
                if len(shortest[0]) <= len(key):
                    ret["isdvc"] = True

            ret["type"] = "directory"
            return ret

        item_key, (meta, hash_info) = outs[0]

        meta = meta or Meta()

        if key != item_key:
            assert item_key[: len(key)] == key
            ret["type"] = "directory"
            return ret

        ret["size"] = meta.size
        ret["isexec"] = meta.isexec
        ret[hash_info.name] = hash_info.value
        ret["isdvc"] = True
        ret["isout"] = True
        ret["meta"] = meta
        if hash_info and hash_info.isdir:
            ret["type"] = "directory"
        return ret

    def get_file(  # pylint: disable=arguments-differ
        self, rpath, lpath, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, path = self._get_fs_path(rpath)
        fs.get_file(path, lpath, callback=callback, **kwargs)

    def checksum(self, path):
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            return md5
        raise NotImplementedError


class DataFileSystem(FileSystem):
    protocol = "local"

    PARAM_CHECKSUM = "md5"

    def _prepare_credentials(self, **config):
        return config

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        return _DataFileSystem(**self.fs_args)

    def isdvc(self, path, **kwargs):
        return self.fs.isdvc(path, **kwargs)

    @property
    def repo(self):
        return self.fs.repo
