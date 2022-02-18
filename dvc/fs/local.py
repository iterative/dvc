import logging
import os

from dvc.scheme import Schemes
from dvc.system import System
from dvc.utils import tmp_fname
from dvc.utils.fs import copy_fobj_to_file, copyfile, makedirs, move, remove

from ._callback import DEFAULT_CALLBACK
from .base import FileSystem

logger = logging.getLogger(__name__)


class LocalFileSystem(FileSystem):
    sep = os.sep

    scheme = Schemes.LOCAL
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, **config):
        from fsspec.implementations.local import LocalFileSystem as LocalFS

        super().__init__(**config)
        self.fs = LocalFS()

    @staticmethod
    def open(path, mode="r", encoding=None, **kwargs):
        return open(path, mode=mode, encoding=encoding)

    def exists(self, path) -> bool:
        # TODO: replace this with os.path.exists once the problem is fixed on
        # the fsspec https://github.com/intake/filesystem_spec/issues/742
        return os.path.lexists(path)

    def checksum(self, path) -> str:
        from fsspec.utils import tokenize

        st = os.stat(path)

        return str(int(tokenize([st.st_ino, st.st_mtime, st.st_size]), 16))

    def isfile(self, path) -> bool:
        return os.path.isfile(path)

    def isdir(self, path) -> bool:
        return os.path.isdir(path)

    def iscopy(self, path):
        return not (System.is_symlink(path) or System.is_hardlink(path))

    def walk(self, top, topdown=True, **kwargs):
        """Directory fs generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        for root, dirs, files in os.walk(
            top,
            topdown=topdown,
        ):
            yield os.path.normpath(root), dirs, files

    def find(self, path, prefix=None):
        for root, _, files in self.walk(path):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{file}"

    def is_empty(self, path):
        if self.isfile(path) and os.path.getsize(path) == 0:
            return True

        if self.isdir(path) and len(os.listdir(path)) == 0:
            return True

        return False

    def remove(self, path):
        remove(path)

    def makedirs(self, path, **kwargs):
        makedirs(path, exist_ok=kwargs.pop("exist_ok", True))

    def move(self, from_info, to_info):
        self.makedirs(self.path.parent(to_info))
        move(from_info, to_info)

    def copy(self, from_info, to_info):
        tmp_info = self.path.join(self.path.parent(to_info), tmp_fname(""))
        try:
            copyfile(from_info, tmp_info)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise

    def upload_fobj(self, fobj, to_info, **kwargs):
        self.makedirs(self.path.parent(to_info))
        tmp_info = self.path.join(self.path.parent(to_info), tmp_fname(""))
        try:
            copy_fobj_to_file(fobj, tmp_info)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise

    @staticmethod
    def symlink(from_info, to_info):
        System.symlink(from_info, to_info)

    @staticmethod
    def is_symlink(path):
        return System.is_symlink(path)

    def hardlink(self, from_info, to_info):
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
        if self.getsize(from_info) == 0:
            self.open(to_info, "w").close()

            logger.debug("Created empty file: %s -> %s", from_info, to_info)
            return

        System.hardlink(from_info, to_info)

    @staticmethod
    def is_hardlink(path):
        return System.is_hardlink(path)

    def reflink(self, from_info, to_info):
        System.reflink(from_info, to_info)

    def info(self, path):
        return self.fs.info(path)

    def put_file(
        self, from_file, to_info, callback=DEFAULT_CALLBACK, **kwargs
    ):
        parent = self.path.parent(to_info)
        makedirs(parent, exist_ok=True)
        tmp_file = self.path.join(parent, tmp_fname())
        copyfile(from_file, tmp_file, callback=callback)
        os.replace(tmp_file, to_info)

    def get_file(
        self, from_info, to_file, callback=DEFAULT_CALLBACK, **kwargs
    ):
        copyfile(from_info, to_file, callback=callback)


localfs = LocalFileSystem()
