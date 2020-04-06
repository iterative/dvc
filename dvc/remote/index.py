import logging
import pathlib
import pickle

logger = logging.getLogger(__name__)


class RemoteIndex(object):
    """Class for locally indexing remote checksums.

    Args:
        repo: repo for this remote
    """

    INDEX_SUFFIX = ".idx"

    def __init__(self, repo, name):
        if repo and hasattr(repo, "index_dir") and name:
            self.path = pathlib.Path(repo.index_dir).joinpath(
                "{}{}".format(name, self.INDEX_SUFFIX)
            )
        else:
            self.path = None
        self._checksums = set()
        self.load()

    def __iter__(self):
        return iter(self._checksums)

    @property
    def checksums(self):
        return self._checksums

    def load(self):
        """(Re)load this index from disk."""
        if self.path and self.path.exists():
            try:
                with open(self.path, "rb") as fd:
                    self._checksums = pickle.load(fd)
            except IOError:
                logger.error(
                    "Failed to load remote index from '{}'".format(self.path)
                )

    def save(self):
        """Save this index to disk."""
        if self.path:
            try:
                with open(self.path, "wb") as fd:
                    pickle.dump(self._checksums, fd)
            except IOError:
                logger.error(
                    "Failed to save remote index to '{}'".format(self.path)
                )

    def invalidate(self):
        """Invalidate this index (to force re-indexing later)."""
        self._checksums.clear()
        if self.path and self.path.exists():
            self.path.unlink()

    def remove(self, checksum):
        if checksum in self._checksums:
            self._checksums.remove(checksum)

    def replace(self, checksums):
        """Replace the full contents of this index with ``checksums``.

        Changes to the index will not be written to disk.
        """
        self._checksums = set(checksums)

    def update(self, *checksums):
        """Update this index, adding elements from ``checksums``.

        Changes to the index will not be written to disk.
        """
        self._checksums.update(*checksums)
