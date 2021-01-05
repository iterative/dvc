import errno
import logging
import os
import stat
from typing import Any, Dict

from funcy import cached_property
from shortuuid import uuid

from dvc.exceptions import DvcException
from dvc.hash_info import HashInfo
from dvc.path_info import PathInfo
from dvc.scheme import Schemes
from dvc.system import System
from dvc.utils import file_md5, is_exec, relpath, tmp_fname
from dvc.utils.fs import copy_fobj_to_file, copyfile, makedirs, move, remove

from .base import BaseTree

logger = logging.getLogger(__name__)


class LocalTree(BaseTree):
    scheme = Schemes.LOCAL
    PATH_CLS = PathInfo
    PARAM_CHECKSUM = "md5"
    PARAM_PATH = "path"
    TRAVERSE_PREFIX_LEN = 2

    CACHE_MODE = 0o444
    SHARED_MODE_MAP: Dict[Any, Any] = {
        None: (0o644, 0o755),
        "group": (0o664, 0o775),
    }

    def __init__(self, repo, config, use_dvcignore=False, dvcignore_root=None):
        super().__init__(repo, config)
        url = config.get("url")
        self.path_info = self.PATH_CLS(url) if url else None
        self.use_dvcignore = use_dvcignore
        self.dvcignore_root = dvcignore_root

    @property
    def tree_root(self):
        return self.config.get("url")

    @property
    def state(self):
        from dvc.state import StateNoop

        return self.repo.state if self.repo else StateNoop()

    @cached_property
    def dvcignore(self):
        from dvc.ignore import DvcIgnoreFilter, DvcIgnoreFilterNoop

        root = self.dvcignore_root or self.tree_root
        cls = DvcIgnoreFilter if self.use_dvcignore else DvcIgnoreFilterNoop
        return cls(self, root)

    @staticmethod
    def open(path_info, mode="r", encoding=None):
        return open(path_info, mode=mode, encoding=encoding)

    def exists(self, path_info, use_dvcignore=True):
        assert isinstance(path_info, str) or path_info.scheme == "local"
        if self.repo:
            ret = os.path.lexists(path_info)
        else:
            ret = os.path.exists(path_info)
        if not ret:
            return False
        if not use_dvcignore:
            return True

        return not self.dvcignore.is_ignored_file(
            path_info
        ) and not self.dvcignore.is_ignored_dir(path_info)

    def isfile(self, path_info):
        if not os.path.isfile(path_info):
            return False

        return not self.dvcignore.is_ignored_file(path_info)

    def isdir(
        self, path_info, use_dvcignore=True
    ):  # pylint: disable=arguments-differ
        if not os.path.isdir(path_info):
            return False
        return not (use_dvcignore and self.dvcignore.is_ignored_dir(path_info))

    def iscopy(self, path_info):
        return not (
            System.is_symlink(path_info) or System.is_hardlink(path_info)
        )

    def walk(
        self,
        top,
        topdown=True,
        onerror=None,
        use_dvcignore=True,
        ignore_subrepos=True,
    ):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        for root, dirs, files in os.walk(
            top, topdown=topdown, onerror=onerror
        ):
            if use_dvcignore:
                dirs[:], files[:] = self.dvcignore(
                    os.path.abspath(root),
                    dirs,
                    files,
                    ignore_subrepos=ignore_subrepos,
                )

            yield os.path.normpath(root), dirs, files

    def walk_files(self, path_info, **kwargs):
        for root, _, files in self.walk(path_info):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield PathInfo(f"{root}{os.sep}{file}")

    def is_empty(self, path_info):
        path = path_info.fspath

        if self.isfile(path_info) and os.path.getsize(path) == 0:
            return True

        if self.isdir(path_info) and len(os.listdir(path)) == 0:
            return True

        return False

    def remove(self, path_info):
        if isinstance(path_info, PathInfo):
            if path_info.scheme != "local":
                raise NotImplementedError
            path = path_info.fspath
        else:
            path = path_info

        if self.exists(path):
            remove(path)

    def makedirs(self, path_info):
        makedirs(path_info, exist_ok=True, mode=self.dir_mode)

    def isexec(self, path):
        mode = os.stat(path).st_mode
        return is_exec(mode)

    def stat(self, path):
        if self.dvcignore.is_ignored(path):
            raise FileNotFoundError

        return os.stat(path)

    def move(self, from_info, to_info, mode=None):
        if from_info.scheme != "local" or to_info.scheme != "local":
            raise NotImplementedError

        self.makedirs(to_info.parent)

        if mode is None:
            if self.isfile(from_info):
                mode = self.file_mode
            else:
                mode = self.dir_mode

        move(from_info, to_info, mode=mode)

    def copy(self, from_info, to_info):
        tmp_info = to_info.parent / tmp_fname("")
        try:
            System.copy(from_info, tmp_info)
            os.chmod(tmp_info, self.file_mode)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise

    def copy_fobj(self, fobj, to_info):
        self.makedirs(to_info.parent)
        tmp_info = to_info.parent / tmp_fname("")
        try:
            copy_fobj_to_file(fobj, tmp_info)
            os.chmod(tmp_info, self.file_mode)
            os.rename(tmp_info, to_info)
        except Exception:
            self.remove(tmp_info)
            raise

    @staticmethod
    def symlink(from_info, to_info):
        System.symlink(from_info, to_info)

    @staticmethod
    def is_symlink(path_info):
        return System.is_symlink(path_info)

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

            logger.debug(
                "Created empty file: {src} -> {dest}".format(
                    src=str(from_info), dest=str(to_info)
                )
            )
            return

        System.hardlink(from_info, to_info)

    @staticmethod
    def is_hardlink(path_info):
        return System.is_hardlink(path_info)

    def reflink(self, from_info, to_info):
        tmp_info = to_info.parent / tmp_fname("")
        System.reflink(from_info, tmp_info)
        # NOTE: reflink has its own separate inode, so you can set permissions
        # that are different from the source.
        os.chmod(tmp_info, self.file_mode)
        os.rename(tmp_info, to_info)

    def chmod(self, path_info, mode):
        path = os.fspath(path_info)
        try:
            os.chmod(path, mode)
        except OSError as exc:
            # There is nothing we need to do in case of a read-only file system
            if exc.errno == errno.EROFS:
                return

            # In shared cache scenario, we might not own the cache file, so we
            # need to check if cache file is already protected.
            if exc.errno not in [errno.EPERM, errno.EACCES]:
                raise

            actual = stat.S_IMODE(os.stat(path).st_mode)
            if actual != mode:
                raise

    def _unprotect_file(self, path):
        if System.is_symlink(path) or System.is_hardlink(path):
            logger.debug(f"Unprotecting '{path}'")
            tmp = os.path.join(os.path.dirname(path), "." + uuid())

            # The operations order is important here - if some application
            # would access the file during the process of copyfile then it
            # would get only the part of file. So, at first, the file should be
            # copied with the temporary name, and then original file should be
            # replaced by new.
            copyfile(path, tmp, name="Unprotecting '{}'".format(relpath(path)))
            remove(path)
            os.rename(tmp, path)

        else:
            logger.debug(
                "Skipping copying for '{}', since it is not "
                "a symlink or a hardlink.".format(path)
            )

        os.chmod(path, self.file_mode)

    def _unprotect_dir(self, path):
        for fname in self.walk_files(path):
            self._unprotect_file(fname)

    def unprotect(self, path_info):
        path = path_info.fspath
        if not os.path.exists(path):
            raise DvcException(f"can't unprotect non-existing data '{path}'")

        if os.path.isdir(path):
            self._unprotect_dir(path)
        else:
            self._unprotect_file(path)

    def protect(self, path_info):
        self.chmod(path_info, self.CACHE_MODE)

    def is_protected(self, path_info):
        try:
            mode = os.stat(path_info).st_mode
        except FileNotFoundError:
            return False

        return stat.S_IMODE(mode) == self.CACHE_MODE

    def get_file_hash(self, path_info):
        hash_info = HashInfo(self.PARAM_CHECKSUM, file_md5(path_info)[0],)

        if hash_info:
            hash_info.size = os.path.getsize(path_info)

        return hash_info

    @staticmethod
    def getsize(path_info):
        return os.path.getsize(path_info)

    def _upload(
        self,
        from_file,
        to_info,
        name=None,
        no_progress_bar=False,
        file_mode=None,
        **_kwargs,
    ):
        makedirs(to_info.parent, exist_ok=True)

        tmp_file = tmp_fname(to_info)
        copyfile(
            from_file, tmp_file, name=name, no_progress_bar=no_progress_bar
        )

        if file_mode is not None:
            self.chmod(tmp_file, file_mode)
        else:
            self.protect(tmp_file)
        os.replace(tmp_file, to_info)

    @staticmethod
    def _download(
        from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        copyfile(
            from_info, to_file, no_progress_bar=no_progress_bar, name=name
        )

    def _reset(self):
        return self.__dict__.pop("dvcignore", None)
