import logging
import os
import typing

from ._callback import DEFAULT_CALLBACK
from .base import FileSystem

if typing.TYPE_CHECKING:
    from dvc.types import AnyPath

logger = logging.getLogger(__name__)


class DvcFileSystem(FileSystem):  # pylint:disable=abstract-method
    """DVC repo fs.

    Args:
        repo: DVC repo.
    """

    sep = os.sep

    scheme = "local"
    PARAM_CHECKSUM = "md5"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.repo = kwargs["repo"]

    @property
    def config(self):
        raise NotImplementedError

    def _get_key(self, path):
        from dvc.fs.local import LocalFileSystem

        from . import get_cloud_fs

        cls, kwargs, fs_path = get_cloud_fs(None, url=path)

        if cls != LocalFileSystem or os.path.isabs(path):
            fs = cls(**kwargs)
            return (cls.scheme, *fs.path.parts(fs_path))

        fs_key = "repo"
        key = self.path.parts(path)
        if key == (".",):
            key = ()

        return (fs_key, *key)

    def _get_fs_path(self, path: "AnyPath", remote=None):
        from dvc.config import NoRemoteError

        info = self.info(path)
        if info["type"] == "directory":
            raise IsADirectoryError

        value = info.get("md5")
        if not value:
            raise FileNotFoundError

        cache_path = self.repo.odb.local.hash_to_path(value)

        if self.repo.odb.local.fs.exists(cache_path):
            return self.repo.odb.local.fs, cache_path

        try:
            remote_odb = self.repo.cloud.get_remote_odb(remote)
        except NoRemoteError as exc:
            raise FileNotFoundError from exc
        remote_fs_path = remote_odb.hash_to_path(value)
        return remote_odb.fs, remote_fs_path

    def open(  # type: ignore
        self, path: str, mode="r", encoding=None, **kwargs
    ):  # pylint: disable=arguments-renamed
        fs, fspath = self._get_fs_path(path, **kwargs)
        return fs.open(fspath, mode=mode, encoding=encoding)

    def exists(self, path):  # pylint: disable=arguments-renamed
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def isdir(self, path):  # pylint: disable=arguments-renamed
        try:
            return self.info(path)["type"] == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path):  # pylint: disable=arguments-renamed
        try:
            return self.info(path)["type"] == "file"
        except FileNotFoundError:
            return False

    def _walk(self, root, topdown=True, **kwargs):
        dirs = set()
        files = []

        root_parts = self._get_key(root)
        root_len = len(root_parts)
        try:
            for key, (meta, hash_info) in self.repo.index.tree.iteritems(
                prefix=root_parts
            ):  # noqa: B301
                if hash_info and hash_info.isdir and meta and not meta.obj:
                    raise FileNotFoundError

                if key == root_parts:
                    continue

                if hash_info.isdir:
                    continue

                name = key[root_len]
                if len(key) > root_len + 1:
                    dirs.add(name)
                    continue

                files.append(name)
        except KeyError:
            pass

        assert topdown
        dirs = list(dirs)
        yield root, dirs, files

        for dname in dirs:
            yield from self._walk(self.path.join(root, dname))

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        assert topdown
        try:
            info = self.info(top)
        except FileNotFoundError:
            if onerror is not None:
                onerror(FileNotFoundError(top))
            return

        if info["type"] != "directory":
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        yield from self._walk(top, topdown=topdown, **kwargs)

    def find(self, path, prefix=None):
        for root, _, files in self.walk(path):
            for fname in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{fname}"

    def isdvc(self, path, recursive=False, strict=True):
        try:
            info = self.info(path)
        except FileNotFoundError:
            return False

        recurse = recursive or not strict
        return bool(info.get("outs") if recurse else info.get("isout"))

    def info(self, path):
        from dvc.data.meta import Meta

        key = self._get_key(path)

        try:
            outs = list(self.repo.index.tree.iteritems(key))
        except KeyError as exc:
            raise FileNotFoundError from exc

        ret = {
            "type": "file",
            "size": 0,
            "isexec": False,
            "isdvc": False,
            "outs": outs,
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

    def get_file(
        self, from_info, to_file, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, path = self._get_fs_path(from_info)
        fs.get_file(  # pylint: disable=protected-access
            path, to_file, callback=callback, **kwargs
        )

    def checksum(self, path):
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            return md5
        raise NotImplementedError
