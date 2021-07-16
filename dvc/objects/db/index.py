import logging
import os
from typing import TYPE_CHECKING, Iterable, Optional, Set

from ..errors import ObjectDBError

if TYPE_CHECKING:
    from dvc.types import StrPath

logger = logging.getLogger(__name__)


class ObjectDBIndexNoop:
    """No-op class for ODBs which are not indexed."""

    def __init__(self, *args, **kwargs):
        pass

    def __iter__(self):
        return iter([])

    def __contains__(self, hash_):
        return False

    @staticmethod
    def hashes():
        return []

    @staticmethod
    def dir_hashes():
        return []

    def clear(self):
        pass

    def update(self, *args):
        pass

    @staticmethod
    def intersection(*args):
        return []


class ObjectDBIndex:
    """Class for indexing hashes in an ODB."""

    INDEX_SUFFIX = ".idx"
    INDEX_DIR = "index"

    def __init__(
        self, tmp_dir: "StrPath", name: str, dir_suffix: Optional[str] = None
    ):
        from diskcache import Index

        from dvc.fs.local import LocalFileSystem
        from dvc.utils.fs import makedirs

        self.index_dir = os.path.join(tmp_dir, self.INDEX_DIR, name)
        makedirs(self.index_dir, exist_ok=True)
        self.fs = LocalFileSystem()
        self.index = Index(self.index_dir)

        if not dir_suffix:
            dir_suffix = self.fs.CHECKSUM_DIR_SUFFIX
        self.dir_suffix = dir_suffix

    def __iter__(self):
        return iter(self.index)

    def __contains__(self, hash_):
        return hash_ in self.index

    def hashes(self):
        """Iterate over hashes stored in the index."""
        return iter(self)

    def dir_hashes(self):
        """Iterate over .dir hashes stored in the index."""
        yield from (hash_ for hash_, is_dir in self.index.items() if is_dir)

    def is_dir_hash(self, hash_: str):
        return hash_.endswith(self.dir_suffix)

    def clear(self):
        """Clear this index (to force re-indexing later)."""
        from diskcache import Timeout

        try:
            self.index.clear()
        except Timeout as exc:
            raise ObjectDBError("Failed to clear ODB index") from exc

    def update(self, dir_hashes: Iterable[str], file_hashes: Iterable[str]):
        """Update this index, adding the specified hashes."""
        from diskcache import Timeout

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
