import logging
import pathlib
import pickle

logger = logging.getLogger(__name__)


class RemoteIndex(object):
    """Class for locally indexing remote checksums.

    Args:
        repo: repo for this remote index.
        name: name for this index. If name is provided, this index will be
            loaded from and saved to ``.dvc/tmp/index/{name}.idx``.
            If name is not provided (i.e. for local remotes), this index will
            be kept in memory but not saved to disk.
    """

    INDEX_SUFFIX = ".idx"

    def __init__(self, repo, name=None):
        if name:
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
        if self.path and self.path.is_file():
            try:
                with open(self.path, "rb") as fobj:
                    self._checksums = pickle.load(fobj)
            except PermissionError:
                logger.error(
                    "Insufficient permissions to read index file "
                    "'{}'".format(self.path)
                )

    def save(self):
        """Save this index to disk."""
        if self.path:
            try:
                with open(self.path, "wb") as fobj:
                    pickle.dump(self._checksums, fobj)
            except PermissionError:
                logger.error(
                    "Insufficient permissions to write index file "
                    "'{}'".format(self.path)
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
