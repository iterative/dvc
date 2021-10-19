import logging
import os
import typing

from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.utils import relpath

from ..progress import DEFAULT_CALLBACK
from ._metadata import Metadata
from .base import BaseFileSystem

if typing.TYPE_CHECKING:
    from dvc.output import Output


logger = logging.getLogger(__name__)


class DvcFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
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

    def _find_outs(self, path, *args, **kwargs):
        outs = self.repo.find_outs_by_path(path, *args, **kwargs)

        def _is_cached(out):
            return out.use_cache

        outs = list(filter(_is_cached, outs))
        if not outs:
            raise OutputNotFoundError(path, self.repo)

        return outs

    def _get_granular_hash(
        self, path_info: PathInfo, out: "Output", remote=None
    ):
        assert isinstance(path_info, PathInfo)
        # NOTE: use string paths here for performance reasons
        key = tuple(relpath(path_info, out.path_info).split(os.sep))
        out.get_dir_cache(remote=remote)
        if out.obj is None:
            raise FileNotFoundError
        (_, oid) = out.obj.trie.get(key) or (None, None)
        if oid:
            return oid
        raise FileNotFoundError

    def _get_fs_path(self, path: PathInfo, remote=None):
        try:
            outs = self._find_outs(path, strict=False)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        if len(outs) != 1 or (
            outs[0].is_dir_checksum and path == outs[0].path_info
        ):
            raise IsADirectoryError

        out = outs[0]

        if not out.hash_info:
            raise FileNotFoundError

        if out.changed_cache(filter_info=path):
            from dvc.config import NoRemoteError

            try:
                remote_odb = self.repo.cloud.get_remote_odb(remote)
            except NoRemoteError as exc:
                raise FileNotFoundError from exc
            if out.is_dir_checksum:
                checksum = self._get_granular_hash(path, out).value
            else:
                checksum = out.hash_info.value
            remote_info = remote_odb.hash_to_path_info(checksum)
            return remote_odb.fs, remote_info

        if out.is_dir_checksum:
            checksum = self._get_granular_hash(path, out).value
            cache_path = out.odb.hash_to_path_info(checksum).url
        else:
            cache_path = out.cache_path
        return out.odb.fs, cache_path

    def open(  # type: ignore
        self, path: PathInfo, mode="r", encoding=None, **kwargs
    ):  # pylint: disable=arguments-renamed
        fs, fspath = self._get_fs_path(path, **kwargs)
        return fs.open(fspath, mode=mode, encoding=encoding)

    def exists(self, path):  # pylint: disable=arguments-renamed
        try:
            self.metadata(path)
            return True
        except FileNotFoundError:
            return False

    def isdir(self, path):  # pylint: disable=arguments-renamed
        try:
            meta = self.metadata(path)
            return meta.isdir
        except FileNotFoundError:
            return False

    def check_isdir(self, path_info, outs):
        if len(outs) != 1:
            return True

        out = outs[0]
        if not out.is_dir_checksum:
            return out.path_info != path_info
        if out.path_info == path_info:
            return True

        try:
            self._get_granular_hash(path_info, out)
            return False
        except FileNotFoundError:
            return True

    def isfile(self, path):  # pylint: disable=arguments-renamed
        try:
            meta = self.metadata(path)
            return meta.isfile
        except FileNotFoundError:
            return False

    def _fetch_dir(self, out, **kwargs):
        # pull dir cache if needed
        out.get_dir_cache(**kwargs)

        if not out.obj:
            raise FileNotFoundError

    def _add_dir(self, trie, out, **kwargs):
        self._fetch_dir(out, **kwargs)

        base = out.path_info.parts
        for key, _, _ in out.obj:  # noqa: B301
            trie[base + key] = None

    def _walk(self, root, trie, topdown=True, **kwargs):
        dirs = set()
        files = []

        out = trie.get(root.parts)
        if out and out.is_dir_checksum:
            self._add_dir(trie, out, **kwargs)

        root_len = len(root.parts)
        try:
            for key, out in trie.iteritems(prefix=root.parts):  # noqa: B301
                if key == root.parts:
                    continue

                name = key[root_len]
                if len(key) > root_len + 1 or (out and out.is_dir_checksum):
                    dirs.add(name)
                    continue

                files.append(name)
        except KeyError:
            pass

        assert topdown
        dirs = list(dirs)
        yield root.fspath, dirs, files

        for dname in dirs:
            yield from self._walk(root / dname, trie)

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        from pygtrie import Trie

        assert topdown
        root = PathInfo(os.path.abspath(top))
        try:
            meta = self.metadata(root)
        except FileNotFoundError:
            if onerror is not None:
                onerror(FileNotFoundError(top))
            return

        if not meta.isdir:
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        trie = Trie()
        for out in meta.outs:
            trie[out.path_info.parts] = out

            if out.is_dir_checksum and root.isin_or_eq(out.path_info):
                self._add_dir(trie, out, **kwargs)

        yield from self._walk(root, trie, topdown=topdown, **kwargs)

    def walk_files(self, path_info, **kwargs):
        for root, _, files in self.walk(path_info):
            for fname in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield PathInfo(f"{root}{os.sep}{fname}")

    def isdvc(self, path, recursive=False, strict=True):
        try:
            meta = self.metadata(path)
        except FileNotFoundError:
            return False

        recurse = recursive or not strict
        return meta.output_exists if recurse else meta.is_output

    def isexec(self, path_info):  # pylint: disable=unused-argument
        return False

    def metadata(self, path_info):
        path_info = PathInfo(os.path.abspath(path_info))

        try:
            outs = self._find_outs(path_info, strict=False, recursive=True)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        meta = Metadata(path_info=path_info, outs=outs, repo=self.repo)
        meta.isdir = meta.isdir or self.check_isdir(meta.path_info, meta.outs)
        return meta

    def info(self, path_info):
        meta = self.metadata(path_info)
        ret = {"type": "directory" if meta.isdir else "file"}
        if meta.is_output and len(meta.outs) == 1 and meta.outs[0].hash_info:
            out = meta.outs[0]
            ret["size"] = out.meta.size
            ret[out.hash_info.name] = out.hash_info.value
        elif meta.part_of_output:
            (out,) = meta.outs
            key = path_info.relative_to(out.path_info).parts
            (obj_meta, oid) = out.obj.trie.get(key) or (None, None)
            if oid:
                ret["size"] = obj_meta.size if obj_meta else 0
                ret[oid.name] = oid.value

        return ret

    def get_file(
        self, from_info, to_file, callback=DEFAULT_CALLBACK, **kwargs
    ):
        fs, path = self._get_fs_path(from_info)
        fs.get_file(  # pylint: disable=protected-access
            path, to_file, callback=callback, **kwargs
        )

    def checksum(self, path_info):
        info = self.info(path_info)
        md5 = info.get("md5")
        if md5:
            return md5
        raise NotImplementedError
