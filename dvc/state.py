"""Manages state database used for checksum caching."""

import logging
import os
from abc import ABC, abstractmethod

from dvc.fs.local import LocalFileSystem
from dvc.hash_info import HashInfo
from dvc.utils import relpath
from dvc.utils.decorators import with_diskcache
from dvc.utils.fs import get_inode, get_mtime_and_size, remove

logger = logging.getLogger(__name__)


class StateBase(ABC):
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def save(self, path, fs, hash_info):
        pass

    @abstractmethod
    def get(self, path, fs):
        pass

    @abstractmethod
    def save_link(self, path, fs):
        pass


class StateNoop(StateBase):
    def close(self):
        pass

    def save(self, path, fs, hash_info):
        pass

    def get(self, path, fs):  # pylint: disable=unused-argument
        return None, None

    def save_link(self, path, fs):
        pass


class State(StateBase):  # pylint: disable=too-many-instance-attributes
    def __init__(self, root_dir=None, tmp_dir=None, dvcignore=None):
        from diskcache import Cache

        super().__init__()

        self.tmp_dir = tmp_dir
        self.root_dir = root_dir
        self.dvcignore = dvcignore

        if not tmp_dir:
            return

        config = {
            "eviction_policy": "least-recently-used",
            "disk_pickle_protocol": 4,
        }
        self.links = Cache(directory=os.path.join(tmp_dir, "links"), **config)
        self.md5s = Cache(directory=os.path.join(tmp_dir, "md5s"), **config)

    def close(self):
        self.md5s.close()
        self.links.close()

    @with_diskcache(name="md5s")
    def save(self, path, fs, hash_info):
        """Save hash for the specified path info.

        Args:
            path (str): path to save hash for.
            hash_info (HashInfo): hash to save.
        """

        if not isinstance(fs, LocalFileSystem):
            return

        mtime, size = get_mtime_and_size(path, fs, self.dvcignore)
        inode = get_inode(path)

        logger.debug(
            "state save (%s, %s, %s) %s",
            inode,
            mtime,
            str(size),
            hash_info.value,
        )

        self.md5s[inode] = (mtime, str(size), hash_info.value)

    @with_diskcache(name="md5s")
    def get(self, path, fs):
        """Gets the hash for the specified path info. Hash will be
        retrieved from the state database if available.

        Args:
            path (str): path info to get the hash for.

        Returns:
            HashInfo or None: hash for the specified path info or None if it
            doesn't exist in the state database.
        """
        from .data.meta import Meta

        if not isinstance(fs, LocalFileSystem):
            return None, None

        try:
            mtime, size = get_mtime_and_size(path, fs, self.dvcignore)
        except FileNotFoundError:
            return None, None

        inode = get_inode(path)

        value = self.md5s.get(inode)

        if not value or value[0] != mtime or value[1] != str(size):
            return None, None

        return Meta(size=size), HashInfo("md5", value[2])

    @with_diskcache(name="links")
    def save_link(self, path, fs):
        """Adds the specified path to the list of links created by dvc. This
        list is later used on `dvc checkout` to cleanup old links.

        Args:
            path (str): path info to add to the list of links.
        """
        if not isinstance(fs, LocalFileSystem):
            return

        try:
            mtime, _ = get_mtime_and_size(path, fs, self.dvcignore)
        except FileNotFoundError:
            return

        inode = get_inode(path)
        relative_path = relpath(path, self.root_dir)

        with self.links as ref:
            ref[relative_path] = (inode, mtime)

    @with_diskcache(name="links")
    def get_unused_links(self, used, fs):
        """Removes all saved links except the ones that are used.

        Args:
            used (list): list of used links that should not be removed.
        """
        if not isinstance(fs, LocalFileSystem):
            return

        unused = []

        with self.links as ref:
            for relative_path in ref:
                path = os.path.join(self.root_dir, relative_path)

                if path in used or not fs.exists(path):
                    continue

                inode = get_inode(path)
                mtime, _ = get_mtime_and_size(path, fs, self.dvcignore)

                if ref[relative_path] == (inode, mtime):
                    logger.debug("Removing '%s' as unused link.", path)
                    unused.append(relative_path)

        return unused

    @with_diskcache(name="links")
    def remove_links(self, unused, fs):
        if not isinstance(fs, LocalFileSystem):
            return

        for path in unused:
            remove(os.path.join(self.root_dir, path))

        with self.links as ref:
            for path in unused:
                del ref[path]
