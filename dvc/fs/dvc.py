import logging
import os
import typing

from dvc.exceptions import OutputNotFoundError
from dvc.utils import relpath

from ..progress import DEFAULT_CALLBACK
from ._metadata import Metadata
from .base import FileSystem

if typing.TYPE_CHECKING:
    from dvc.output import Output
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

    def _find_outs(self, path, *args, **kwargs):
        outs = self.repo.find_outs_by_path(path, *args, **kwargs)

        def _is_cached(out):
            return out.use_cache

        outs = list(filter(_is_cached, outs))
        if not outs:
            raise OutputNotFoundError(path, self.repo)

        return outs

    def _get_granular_hash(self, path: "AnyPath", out: "Output", remote=None):
        # NOTE: use string paths here for performance reasons
        key = tuple(relpath(path, out.fs_path).split(os.sep))
        out.get_dir_cache(remote=remote)
        if out.obj is None:
            raise FileNotFoundError
        (_, oid) = out.obj.trie.get(key) or (None, None)
        if oid:
            return oid
        raise FileNotFoundError

    def _get_fs_path(self, path: "AnyPath", remote=None):
        try:
            outs = self._find_outs(path, strict=False)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        if len(outs) != 1 or (
            outs[0].is_dir_checksum and path == outs[0].fs_path
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
            remote_fs_path = remote_odb.hash_to_path(checksum)
            return remote_odb.fs, remote_fs_path

        if out.is_dir_checksum:
            checksum = self._get_granular_hash(path, out).value
            cache_path = out.odb.fs.unstrip_protocol(
                out.odb.hash_to_path(checksum)
            )
        else:
            cache_path = out.cache_path
        return out.odb.fs, cache_path

    def open(  # type: ignore
        self, path: str, mode="r", encoding=None, **kwargs
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

    def check_isdir(self, path, outs):
        if len(outs) != 1:
            return True

        out = outs[0]
        if not out.is_dir_checksum:
            return out.fs_path != path
        if out.fs_path == path:
            return True

        try:
            self._get_granular_hash(path, out)
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

        base = out.fs.path.parts(out.fs_path)
        for key, _, _ in out.obj:  # noqa: B301
            trie[base + key] = None

    def _walk(self, root, trie, topdown=True, **kwargs):
        dirs = set()
        files = []

        root_parts = self.path.parts(root)
        out = trie.get(root_parts)
        if out and out.is_dir_checksum:
            self._add_dir(trie, out, **kwargs)

        root_len = len(root_parts)
        try:
            for key, out in trie.iteritems(prefix=root_parts):  # noqa: B301
                if key == root_parts:
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
        yield root, dirs, files

        for dname in dirs:
            yield from self._walk(self.path.join(root, dname), trie)

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        from pygtrie import Trie

        assert topdown
        root = os.path.abspath(top)
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
            trie[out.fs.path.parts(out.fs_path)] = out

            if out.is_dir_checksum and self.path.isin_or_eq(root, out.fs_path):
                self._add_dir(trie, out, **kwargs)

        yield from self._walk(root, trie, topdown=topdown, **kwargs)

    def find(self, path, prefix=None):
        for root, _, files in self.walk(path):
            for fname in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{fname}"

    def isdvc(self, path, recursive=False, strict=True):
        try:
            meta = self.metadata(path)
        except FileNotFoundError:
            return False

        recurse = recursive or not strict
        return meta.output_exists if recurse else meta.is_output

    def isexec(self, path):  # pylint: disable=unused-argument
        return False

    def metadata(self, fs_path):
        abspath = os.path.abspath(fs_path)

        try:
            outs = self._find_outs(abspath, strict=False, recursive=True)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        meta = Metadata(fs_path=abspath, outs=outs, repo=self.repo)
        meta.isdir = meta.isdir or self.check_isdir(meta.fs_path, meta.outs)
        return meta

    def info(self, path):
        meta = self.metadata(path)
        ret = {"type": "directory" if meta.isdir else "file"}
        if meta.is_output and len(meta.outs) == 1 and meta.outs[0].hash_info:
            out = meta.outs[0]
            ret["size"] = out.meta.size
            ret[out.hash_info.name] = out.hash_info.value
        elif meta.part_of_output:
            (out,) = meta.outs
            key = self.path.parts(self.path.relpath(path, out.fs_path))
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

    def checksum(self, path):
        info = self.info(path)
        md5 = info.get("md5")
        if md5:
            return md5
        raise NotImplementedError
