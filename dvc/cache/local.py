import logging
import os

from funcy import cached_property
from shortuuid import uuid

from dvc.hash_info import HashInfo
from dvc.path_info import PathInfo
from dvc.progress import Tqdm

from ..utils import relpath
from ..utils.fs import copyfile, remove, walk_files
from .base import CloudCache

logger = logging.getLogger(__name__)


class LocalCache(CloudCache):
    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_MODE = 0o444
    UNPACKED_DIR_SUFFIX = ".unpacked"

    def __init__(self, tree):
        super().__init__(tree)
        self.cache_dir = tree.config.get("url")

    @property
    def cache_dir(self):
        return self.tree.path_info.fspath if self.tree.path_info else None

    @cache_dir.setter
    def cache_dir(self, value):
        self.tree.path_info = PathInfo(value) if value else None

    @cached_property
    def cache_path(self):
        return os.path.abspath(self.cache_dir)

    def hash_to_path(self, hash_):
        # NOTE: `self.cache_path` is already normalized so we can simply use
        # `os.sep` instead of `os.path.join`. This results in this helper
        # being ~5.5 times faster.
        return f"{self.cache_path}{os.sep}{hash_[0:2]}{os.sep}{hash_[2:]}"

    def hashes_exist(
        self, hashes, jobs=None, name=None
    ):  # pylint: disable=unused-argument
        return [
            hash_
            for hash_ in Tqdm(
                hashes,
                unit="file",
                desc="Querying "
                + ("cache in " + name if name else "local cache"),
            )
            if not self.changed_cache_file(
                HashInfo(self.tree.PARAM_CHECKSUM, hash_)
            )
        ]

    def already_cached(self, path_info):
        assert path_info.scheme in ["", "local"]

        return super().already_cached(path_info)

    def _verify_link(self, path_info, link_type):
        if link_type == "hardlink" and self.tree.getsize(path_info) == 0:
            return

        super()._verify_link(path_info, link_type)

    def _list_paths(self, prefix=None, progress_callback=None):
        assert self.tree.path_info is not None
        if prefix:
            path_info = self.tree.path_info / prefix[:2]
            if not self.tree.exists(path_info):
                return
        else:
            path_info = self.tree.path_info
        # NOTE: use utils.fs walk_files since tree.walk_files will not follow
        # symlinks
        if progress_callback:
            for path in walk_files(path_info):
                progress_callback()
                yield path
        else:
            yield from walk_files(path_info)

    def _remove_unpacked_dir(self, hash_):
        info = self.tree.hash_to_path_info(hash_)
        path_info = info.with_name(info.name + self.UNPACKED_DIR_SUFFIX)
        self.tree.remove(path_info)

    def _unprotect_file(self, path):
        if self.tree.is_symlink(path) or self.tree.is_hardlink(path):
            logger.debug("Unprotecting '%s'", path)
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
                "Skipping copying for '%s', since it is not "
                "a symlink or a hardlink.",
                path,
            )

        os.chmod(path, self.tree.file_mode)

    def _unprotect_dir(self, path):
        for fname in self.tree.walk_files(path):
            self._unprotect_file(fname)

    def unprotect(self, path_info):
        if not os.path.exists(path_info):
            from dvc.exceptions import DvcException

            raise DvcException(
                f"can't unprotect non-existing data '{path_info}'"
            )

        if os.path.isdir(path_info):
            self._unprotect_dir(path_info)
        else:
            self._unprotect_file(path_info)

    def protect(self, path_info):
        self.tree.chmod(path_info, self.CACHE_MODE)

    def is_protected(self, path_info):
        import stat

        try:
            mode = os.stat(path_info).st_mode
        except FileNotFoundError:
            return False

        return stat.S_IMODE(mode) == self.CACHE_MODE
