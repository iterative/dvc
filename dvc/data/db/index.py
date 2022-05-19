import logging
import os
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterable, Set

from dvc_objects.errors import ObjectDBError

if TYPE_CHECKING:
    from dvc_objects.fs.base import AnyFSPath

logger = logging.getLogger(__name__)


class ObjectDBIndexBase(ABC):
    @abstractmethod
    def __init__(
        self,
        tmp_dir: "AnyFSPath",
        name: str,
    ):
        pass

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __contains__(self, hash_):
        pass

    def hashes(self):
        return iter(self)

    @abstractmethod
    def dir_hashes(self):
        pass

    @abstractmethod
    def clear(self):
        pass

    @abstractmethod
    def update(self, dir_hashes: Iterable[str], file_hashes: Iterable[str]):
        pass

    @abstractmethod
    def intersection(self, hashes: Set[str]):
        pass


class ObjectDBIndexNoop(ObjectDBIndexBase):
    """No-op class for ODBs which are not indexed."""

    def __init__(
        self,
        tmp_dir: "AnyFSPath",
        name: str,
    ):  # pylint: disable=super-init-not-called
        pass

    def __iter__(self):
        return iter([])

    def __contains__(self, hash_):
        return False

    def dir_hashes(self):
        return []

    def clear(self):
        pass

    def update(self, dir_hashes: Iterable[str], file_hashes: Iterable[str]):
        pass

    def intersection(self, hashes: Set[str]):
        return []


class ObjectDBIndex(ObjectDBIndexBase):
    """Class for indexing hashes in an ODB."""

    INDEX_SUFFIX = ".idx"
    INDEX_DIR = "index"

    def __init__(
        self,
        tmp_dir: "AnyFSPath",
        name: str,
    ):  # pylint: disable=super-init-not-called
        from dvc_objects.cache import Cache, Index
        from dvc_objects.fs import LocalFileSystem
        from dvc_objects.fs.utils import makedirs

        self.index_dir = os.path.join(tmp_dir, self.INDEX_DIR, name)
        makedirs(self.index_dir, exist_ok=True)
        self.fs = LocalFileSystem()
        cache = Cache(self.index_dir, eviction_policy="none", type="index")
        self.index = Index.fromcache(cache)

    def __iter__(self):
        return iter(self.index)

    def __contains__(self, hash_):
        return hash_ in self.index

    def dir_hashes(self):
        """Iterate over .dir hashes stored in the index."""
        yield from (hash_ for hash_, is_dir in self.index.items() if is_dir)

    def clear(self):
        """Clear this index (to force re-indexing later)."""
        from dvc_objects.cache import Timeout

        try:
            self.index.clear()
        except Timeout as exc:
            raise ObjectDBError("Failed to clear ODB index") from exc

    def update(self, dir_hashes: Iterable[str], file_hashes: Iterable[str]):
        """Update this index, adding the specified hashes."""
        from dvc_objects.cache import Timeout

        try:
            with self.index.transact():
                for hash_ in dir_hashes:
                    self.index[hash_] = True
            with self.index.transact():
                for hash_ in file_hashes:
                    self.index[hash_] = False
        except Timeout as exc:
            raise ObjectDBError("Failed to update ODB index") from exc

    def intersection(self, hashes: Set[str]):
        """Iterate over values from `hashes` which exist in the index."""
        yield from hashes.intersection(self.index.keys())
