import logging
import os
import sqlite3
import threading

from funcy import lchunks

from dvc.state import _connect_sqlite

logger = logging.getLogger(__name__)


class RemoteIndexNoop:
    """No-op class for remotes which are not indexed (i.e. local)."""

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, typ, value, tbck):
        pass

    def __iter__(self):
        return iter([])

    def __contains__(self, checksum):
        return False

    @staticmethod
    def checksums():
        return []

    @staticmethod
    def dir_checksums():
        return []

    def load(self):
        pass

    def dump(self):
        pass

    def clear(self):
        pass

    def update(self, *args):
        pass

    @staticmethod
    def intersection(*args):
        return []


class RemoteIndex:
    """Class for indexing remote checksums in a sqlite3 database.

    Args:
        repo: repo for this remote index.
        name: name for this index. Index db will be loaded from and saved to
            ``.dvc/tmp/index/{name}.idx``.
        dir_suffix: suffix used for naming directory checksums
    """

    INDEX_SUFFIX = ".idx"
    VERSION = 1
    INDEX_TABLE = "remote_index"
    INDEX_TABLE_LAYOUT = "checksum TEXT PRIMARY KEY, " "dir INTEGER NOT NULL"

    def __init__(self, repo, name, dir_suffix=".dir"):
        self.path = os.path.join(
            repo.index_dir, "{}{}".format(name, self.INDEX_SUFFIX)
        )

        self.dir_suffix = dir_suffix
        self.database = None
        self.cursor = None
        self.modified = False
        self.lock = threading.Lock()

    def __iter__(self):
        cmd = "SELECT checksum FROM {}".format(self.INDEX_TABLE)
        for (checksum,) in self._execute(cmd):
            yield checksum

    def __enter__(self):
        self.lock.acquire()
        self.load()

    def __exit__(self, typ, value, tbck):
        self.dump()
        self.lock.release()

    def __contains__(self, checksum):
        cmd = "SELECT checksum FROM {} WHERE checksum = (?)".format(
            self.INDEX_TABLE
        )
        self._execute(cmd, (checksum,))
        return self.cursor.fetchone() is not None

    def checksums(self):
        """Iterate over checksums stored in the index."""
        return iter(self)

    def dir_checksums(self):
        """Iterate over .dir checksums stored in the index."""
        cmd = "SELECT checksum FROM {} WHERE dir = 1".format(self.INDEX_TABLE)
        for (checksum,) in self._execute(cmd):
            yield checksum

    def is_dir_checksum(self, checksum):
        return checksum.endswith(self.dir_suffix)

    def _execute(self, cmd, parameters=()):
        return self.cursor.execute(cmd, parameters)

    def _executemany(self, cmd, seq_of_parameters):
        return self.cursor.executemany(cmd, seq_of_parameters)

    def _prepare_db(self, empty=False):
        if not empty:
            cmd = "PRAGMA user_version;"
            self._execute(cmd)
            ret = self.cursor.fetchall()
            assert len(ret) == 1
            assert len(ret[0]) == 1
            assert isinstance(ret[0][0], int)
            version = ret[0][0]

            if version != self.VERSION:
                logger.error(
                    "Index file version '{}' will be reformatted "
                    "to the current version '{}'.".format(
                        version, self.VERSION,
                    )
                )
                cmd = "DROP TABLE IF EXISTS {};"
                self._execute(cmd.format(self.INDEX_TABLE))

        cmd = "CREATE TABLE IF NOT EXISTS {} ({})"
        self._execute(cmd.format(self.INDEX_TABLE, self.INDEX_TABLE_LAYOUT))

        cmd = "PRAGMA user_version = {};"
        self._execute(cmd.format(self.VERSION))

    def load(self):
        """(Re)load this index database."""
        retries = 1
        while True:
            assert self.database is None
            assert self.cursor is None

            empty = not os.path.isfile(self.path)
            self.database = _connect_sqlite(self.path, {"nolock": 1})
            self.cursor = self.database.cursor()

            try:
                self._prepare_db(empty=empty)
                return
            except sqlite3.DatabaseError:
                self.cursor.close()
                self.database.close()
                self.database = None
                self.cursor = None
                if retries > 0:
                    os.unlink(self.path)
                    retries -= 1
                else:
                    raise

    def dump(self):
        """Save this index database."""
        assert self.database is not None

        self.database.commit()
        self.cursor.close()
        self.database.close()
        self.database = None
        self.cursor = None

    def clear(self):
        """Clear this index (to force re-indexing later).

        Changes to the index will not committed until dump() is called.
        """
        cmd = "DELETE FROM {}".format(self.INDEX_TABLE)
        self._execute(cmd)

    def update(self, dir_checksums, file_checksums):
        """Update this index, adding the specified checksums.

        Changes to the index will not committed until dump() is called.
        """
        cmd = "INSERT OR IGNORE INTO {} (checksum, dir) VALUES (?, ?)".format(
            self.INDEX_TABLE
        )
        self._executemany(
            cmd, ((checksum, True) for checksum in dir_checksums)
        )
        self._executemany(
            cmd, ((checksum, False) for checksum in file_checksums)
        )

    def intersection(self, checksums):
        """Iterate over values from `checksums` which exist in the index."""
        # sqlite has a compile time limit of 999, see:
        # https://www.sqlite.org/c3ref/c_limit_attached.html#sqlitelimitvariablenumber
        for chunk in lchunks(999, checksums):
            cmd = "SELECT checksum FROM {} WHERE checksum IN ({})".format(
                self.INDEX_TABLE, ",".join("?" for checksum in chunk)
            )
            for (checksum,) in self._execute(cmd, chunk):
                yield checksum
