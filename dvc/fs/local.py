import logging
import os
import threading

from fsspec import AbstractFileSystem
from funcy import cached_property, wrap_prop

from dvc.scheme import Schemes
from dvc.system import System
from dvc.utils import tmp_fname
from dvc.utils.fs import copy_fobj_to_file, copyfile, makedirs, move, remove

from .base import FileSystem

logger = logging.getLogger(__name__)


# pylint:disable=abstract-method, arguments-differ
class FsspecLocalFileSystem(AbstractFileSystem):
    sep = os.sep

    def __init__(self, *args, **kwargs):
        from fsspec.implementations.local import LocalFileSystem as LocalFS

        super().__init__(*args, **kwargs)
        self.fs = LocalFS()

    def makedirs(self, path, exist_ok=False):
        makedirs(path, exist_ok=exist_ok)

    def mkdir(self, path, create_parents=True, **kwargs):
        if self.exists(path):
            raise FileExistsError(path)
        if create_parents:
            self.makedirs(path, exist_ok=True)
        else:
            os.mkdir(path, **kwargs)

    def lexists(self, path, **kwargs):
        return os.path.lexists(path)

    def exists(self, path, **kwargs):
        # TODO: replace this with os.path.exists once the problem is fixed on
        # the fsspec https://github.com/intake/filesystem_spec/issues/742
        return os.path.lexists(path)

    def checksum(self, path) -> str:
        from fsspec.utils import tokenize

        st = os.stat(path)
        return str(int(tokenize([st.st_ino, st.st_mtime, st.st_size]), 16))

    def info(self, path, **kwargs):
        return self.fs.info(path)

    def ls(self, path, **kwargs):
        return self.fs.ls(path, **kwargs)

    def isfile(self, path) -> bool:
        return os.path.isfile(path)

    def isdir(self, path) -> bool:
        return os.path.isdir(path)

    def walk(self, path, maxdepth=None, topdown=True, **kwargs):
        """Directory fs generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        for root, dirs, files in os.walk(
            path,
            topdown=topdown,
        ):
            yield os.path.normpath(root), dirs, files

    def find(self, path, **kwargs):
        for root, _, files in self.walk(path, **kwargs):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{file}"

    @classmethod
    def _parent(cls, path):
        return os.path.dirname(path)

    def put_file(self, lpath, rpath, callback=None, **kwargs):
        parent = self._parent(rpath)
        makedirs(parent, exist_ok=True)
        tmp_file = os.path.join(parent, tmp_fname())
        copyfile(lpath, tmp_file, callback=callback)
        os.replace(tmp_file, rpath)

    def get_file(self, rpath, lpath, callback=None, **kwargs):
        copyfile(rpath, lpath, callback=callback)

    def mv(self, path1, path2, **kwargs):
        self.makedirs(self._parent(path2), exist_ok=True)
        move(path1, path2)

    def rmdir(self, path):
        os.rmdir(path)

    def rm_file(self, path):
        remove(path)

    def rm(self, path, recursive=False, maxdepth=None):
        remove(path)

    def copy(self, path1, path2, recursive=False, on_error=None, **kwargs):
        tmp_info = os.path.join(self._parent(path2), tmp_fname(""))
        try:
            copyfile(path1, tmp_info)
            os.rename(tmp_info, path2)
        except Exception:
            self.rm_file(tmp_info)
            raise

    def open(self, path, mode="r", encoding=None, **kwargs):
        return open(path, mode=mode, encoding=encoding)

    def symlink(self, path1, path2):
        return System.symlink(path1, path2)

    @staticmethod
    def is_symlink(path):
        return System.is_symlink(path)

    @staticmethod
    def is_hardlink(path):
        return System.is_hardlink(path)

    def hardlink(self, path1, path2):
        # If there are a lot of empty files (which happens a lot in datasets),
        # and the cache type is `hardlink`, we might reach link limits and
        # will get something like: `too many links error`
        #
        # This is because all those empty files will have the same hash
        # (i.e. 68b329da9893e34099c7d8ad5cb9c940), therefore, they will be
        # linked to the same file in the cache.
        #
        # From https://en.wikipedia.org/wiki/Hard_link
        #   * ext4 limits the number of hard links on a file to 65,000
        #   * Windows with NTFS has a limit of 1024 hard links on a file
        #
        # That's why we simply create an empty file rather than a link.
        if self.size(path1) == 0:
            self.open(path2, "w").close()

            logger.debug("Created empty file: %s -> %s", path1, path2)
            return

        return System.hardlink(path1, path2)

    def reflink(self, path1, path2):
        return System.reflink(path1, path2)


class LocalFileSystem(FileSystem):
    sep = os.sep

    scheme = Schemes.LOCAL
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"
    TRAVERSE_PREFIX_LEN = 2

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        return FsspecLocalFileSystem(**self.config)

    @cached_property
    def path(self):
        from .path import Path

        return Path(self.sep, os.getcwd)

    def upload_fobj(self, fobj, to_info, **kwargs):
        self.makedirs(self.path.parent(to_info))
        tmp_info = self.path.join(self.path.parent(to_info), tmp_fname(""))
        try:
            copy_fobj_to_file(fobj, tmp_info)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise


localfs = LocalFileSystem()
