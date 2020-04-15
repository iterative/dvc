import logging
import os
import pickle
import threading

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
            self.path = os.path.join(
                repo.index_dir, "{}{}".format(name, self.INDEX_SUFFIX)
            )
        else:
            self.path = None
        self.lock = threading.Lock()
        self._checksums = set()
        self.modified = False
        self.load()

    def __iter__(self):
        return iter(self._checksums)

    @property
    def checksums(self):
        return self._checksums

    def load(self):
        """(Re)load this index from disk."""
        if self.path and os.path.isfile(self.path):
            self.lock.acquire()
            try:
                with open(self.path, "rb") as fobj:
                    self._checksums = pickle.load(fobj)
                self.modified = False
            except IOError as exc:
                logger.error(
                    "Failed to load remote index file '{}'. "
                    "Remote will be re-indexed: '{}'".format(self.path, exc)
                )
            finally:
                self.lock.release()

    def save(self):
        """Save this index to disk."""
        if self.path and self.modified:
            self.lock.acquire()
            try:
                with open(self.path, "wb") as fobj:
                    pickle.dump(self._checksums, fobj)
                self.modified = False
            except IOError as exc:
                logger.error(
                    "Failed to save remote index file '{}': {}".format(
                        self.path, exc
                    )
                )
            finally:
                self.lock.release()

    def invalidate(self):
        """Invalidate this index (to force re-indexing later)."""
        self.lock.acquire()
        self._checksums.clear()
        self.modified = True
        if self.path and os.path.isfile(self.path):
            try:
                os.unlink(self.path)
            except IOError as exc:
                logger.error(
                    "Failed to remove remote index file '{}': {}".format(
                        self.path, exc
                    )
                )
        self.lock.release()

    def remove(self, checksum):
        if checksum in self._checksums:
            self.lock.acquire()
            self._checksums.remove(checksum)
            self.modified = True
            self.lock.release()

    def replace(self, checksums):
        """Replace the full contents of this index with ``checksums``.

        Changes to the index will not be written to disk.
        """
        self.lock.acquire()
        self._checksums = set(checksums)
        self.modified = True
        self.lock.release()

    def update(self, *checksums):
        """Update this index, adding elements from ``checksums``.

        Changes to the index will not be written to disk.
        """
        self.lock.acquire()
        self._checksums.update(*checksums)
        self.modified = True
        self.lock.release()
