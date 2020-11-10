import logging
import os

from funcy import cached_property

from dvc.hash_info import HashInfo
from dvc.path_info import PathInfo
from dvc.progress import Tqdm

from ..tree.local import LocalTree
from ..utils.fs import walk_files
from .base import CloudCache

logger = logging.getLogger(__name__)


class LocalCache(CloudCache):
    DEFAULT_CACHE_TYPES = ["reflink", "copy"]
    CACHE_MODE = LocalTree.CACHE_MODE
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
