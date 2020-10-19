"""Manages state database used for checksum caching."""

import logging
import os
import re
import sqlite3
from abc import ABC, abstractmethod
from urllib.parse import urlencode, urlunparse

from dvc.exceptions import DvcException
from dvc.hash_info import HashInfo
from dvc.utils import current_timestamp, relpath, to_chunks
from dvc.utils.fs import get_inode, get_mtime_and_size, remove

SQLITE_MAX_VARIABLES_NUMBER = 999

logger = logging.getLogger(__name__)


class StateVersionTooNewError(DvcException):
    """Thrown when DVC version is older than the state database version."""

    def __init__(self, dvc_version, expected, actual):
        super().__init__(
            "you are using an old version '{dvc_version}' of DVC that is "
            "using state file version '{expected}', which is not compatible "
            "with the state file version '{actual}', that is used in this "
            "repo. Please upgrade right now!".format(
                dvc_version=dvc_version, expected=expected, actual=actual
            )
        )


class StateBase(ABC):
    def __init__(self):
        self.count = 0

    @property
    @abstractmethod
    def files(self):
        pass

    @abstractmethod
    def save(self, path_info, hash_info):
        pass

    @abstractmethod
    def get(self, path_info):
        pass

    @abstractmethod
    def save_link(self, path_info):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def dump(self):
        pass

    def __enter__(self):
        if self.count <= 0:
            self.load()
        self.count += 1

    def __exit__(self, typ, value, tbck):
        self.count -= 1
        if self.count > 0:
            return
        self.dump()


class StateNoop(StateBase):
    @property
    def files(self):
        return []

    def save(self, path_info, hash_info):
        pass

    def get(self, path_info):  # pylint: disable=unused-argument
        return None

    def save_link(self, path_info):
        pass

    def load(self):
        pass

    def dump(self):
        pass


class State(StateBase):  # pylint: disable=too-many-instance-attributes
    """Class for the state database.

    Args:
        repo (dvc.repo.Repo): repo instance that this state belongs to.
        config (configobj.ConfigObj): config for the state.

    Raises:
        StateVersionTooNewError: thrown when dvc version is older than the
            state database version.
    """

    VERSION = 3
    STATE_FILE = "state"
    STATE_TABLE = "state"
    STATE_TABLE_LAYOUT = (
        "inode INTEGER PRIMARY KEY, "
        "mtime TEXT NOT NULL, "
        "size TEXT NOT NULL, "
        "md5 TEXT NOT NULL, "
        "timestamp TEXT NOT NULL"
    )

    STATE_INFO_TABLE = "state_info"
    STATE_INFO_TABLE_LAYOUT = "count INTEGER"
    STATE_INFO_ROW = 1

    LINK_STATE_TABLE = "link_state"
    LINK_STATE_TABLE_LAYOUT = (
        "path TEXT PRIMARY KEY, "
        "inode INTEGER NOT NULL, "
        "mtime TEXT NOT NULL"
    )

    STATE_ROW_LIMIT = 100000000
    STATE_ROW_CLEANUP_QUOTA = 50

    MAX_INT = 2 ** 63 - 1
    MAX_UINT = 2 ** 64 - 2

    def __init__(self, repo):
        from dvc.tree.local import LocalTree

        super().__init__()

        self.repo = repo
        self.root_dir = repo.root_dir
        self.tree = LocalTree(None, {"url": self.root_dir})

        state_config = repo.config.get("state", {})
        self.row_limit = state_config.get("row_limit", self.STATE_ROW_LIMIT)
        self.row_cleanup_quota = state_config.get(
            "row_cleanup_quota", self.STATE_ROW_CLEANUP_QUOTA
        )

        if not repo.tmp_dir:
            self.state_file = None
            return

        self.state_file = os.path.join(repo.tmp_dir, self.STATE_FILE)

        # https://www.sqlite.org/tempfiles.html
        self.temp_files = [
            self.state_file + "-journal",
            self.state_file + "-wal",
        ]

        self.database = None
        self.cursor = None
        self.inserts = 0

    @property
    def files(self):
        return self.temp_files + [self.state_file]

    def _execute(self, cmd, parameters=()):
        logger.trace(cmd)
        return self.cursor.execute(cmd, parameters)

    def _fetchall(self):
        ret = self.cursor.fetchall()
        logger.debug("fetched: %s", ret)
        return ret

    def _to_sqlite(self, num):
        assert num >= 0
        assert num < self.MAX_UINT
        # NOTE: sqlite stores unit as signed ints, so maximum uint is 2^63-1
        # see http://jakegoulding.com/blog/2011/02/06/sqlite-64-bit-integers/
        if num > self.MAX_INT:
            ret = -(num - self.MAX_INT)
        else:
            ret = num
        assert self._from_sqlite(ret) == num
        return ret

    def _from_sqlite(self, num):
        assert abs(num) <= self.MAX_INT
        if num < 0:
            return abs(num) + self.MAX_INT
        assert num < self.MAX_UINT
        assert num >= 0
        return num

    def _prepare_db(self, empty=False):
        from dvc import __version__

        if not empty:
            cmd = "PRAGMA user_version;"
            self._execute(cmd)
            ret = self._fetchall()
            assert len(ret) == 1
            assert len(ret[0]) == 1
            assert isinstance(ret[0][0], int)
            version = ret[0][0]

            if version > self.VERSION:
                raise StateVersionTooNewError(
                    __version__, self.VERSION, version
                )
            elif version < self.VERSION:
                logger.warning(
                    "State file version '%d' is too old. "
                    "Reformatting to the current version '%d'.",
                    version,
                    self.VERSION,
                )
                cmd = "DROP TABLE IF EXISTS {};"
                self._execute(cmd.format(self.STATE_TABLE))
                self._execute(cmd.format(self.STATE_INFO_TABLE))
                self._execute(cmd.format(self.LINK_STATE_TABLE))

        # Check that the state file is indeed a database
        cmd = "CREATE TABLE IF NOT EXISTS {} ({})"
        self._execute(cmd.format(self.STATE_TABLE, self.STATE_TABLE_LAYOUT))
        self._execute(
            cmd.format(self.STATE_INFO_TABLE, self.STATE_INFO_TABLE_LAYOUT)
        )
        self._execute(
            cmd.format(self.LINK_STATE_TABLE, self.LINK_STATE_TABLE_LAYOUT)
        )

        cmd = (
            "INSERT OR IGNORE INTO {} (count) SELECT 0 "
            "WHERE NOT EXISTS (SELECT * FROM {})"
        )
        self._execute(cmd.format(self.STATE_INFO_TABLE, self.STATE_INFO_TABLE))

        cmd = "PRAGMA user_version = {};"
        self._execute(cmd.format(self.VERSION))

    def load(self):
        """Loads state database."""
        retries = 1
        while True:
            assert self.database is None
            assert self.cursor is None
            assert self.inserts == 0
            empty = not os.path.exists(self.state_file)
            # NOTE: we use nolock option because fcntl() lock sqlite uses
            # doesn't work on some older NFS/CIFS filesystems.
            # This opens a possibility of data corruption by concurrent writes,
            # which is prevented by repo lock.
            self.database = _connect_sqlite(self.state_file, {"nolock": 1})
            self.cursor = self.database.cursor()

            # Try loading once to check that the file is indeed a database
            # and reformat it if it is not.
            try:
                self._prepare_db(empty=empty)
                return
            except sqlite3.DatabaseError:
                self.cursor.close()
                self.database.close()
                self.database = None
                self.cursor = None
                self.inserts = 0
                if retries > 0:
                    os.unlink(self.state_file)
                    retries -= 1
                else:
                    raise

    def _vacuum(self):
        # NOTE: see https://bugs.python.org/issue28518
        self.database.isolation_level = None
        self._execute("VACUUM")
        self.database.isolation_level = ""

    def dump(self):
        """Saves state database."""
        assert self.database is not None

        cmd = "SELECT count from {} WHERE rowid=?".format(
            self.STATE_INFO_TABLE
        )
        self._execute(cmd, (self.STATE_INFO_ROW,))
        ret = self._fetchall()
        assert len(ret) == 1
        assert len(ret[0]) == 1
        count = self._from_sqlite(ret[0][0]) + self.inserts

        if count > self.row_limit:
            msg = "cleaning up state, this might take a while."
            logger.warning(msg)

            delete = count - self.row_limit
            delete += int(self.row_limit * (self.row_cleanup_quota / 100.0))
            cmd = (
                "DELETE FROM {} WHERE timestamp IN ("
                "SELECT timestamp FROM {} ORDER BY timestamp ASC LIMIT {});"
            )
            self._execute(
                cmd.format(self.STATE_TABLE, self.STATE_TABLE, delete)
            )

            self._vacuum()

            cmd = "SELECT COUNT(*) FROM {}"

            self._execute(cmd.format(self.STATE_TABLE))
            ret = self._fetchall()
            assert len(ret) == 1
            assert len(ret[0]) == 1
            count = ret[0][0]

        cmd = "UPDATE {} SET count = ? WHERE rowid = ?".format(
            self.STATE_INFO_TABLE
        )
        self._execute(cmd, (self._to_sqlite(count), self.STATE_INFO_ROW))

        self.database.commit()
        self.cursor.close()
        self.database.close()
        self.database = None
        self.cursor = None
        self.inserts = 0

    @staticmethod
    def _file_metadata_changed(actual_mtime, mtime, actual_size, size):
        return actual_mtime != mtime or actual_size != size

    def _update_state_record_timestamp_for_inode(self, actual_inode):
        cmd = "UPDATE {} SET timestamp = ? WHERE inode = ?".format(
            self.STATE_TABLE
        )
        self._execute(
            cmd, (current_timestamp(), self._to_sqlite(actual_inode))
        )

    def _update_state_for_path_changed(
        self, actual_inode, actual_mtime, actual_size, checksum
    ):
        cmd = (
            "UPDATE {} SET "
            "mtime = ?, size = ?, "
            "md5 = ?, timestamp = ? "
            "WHERE inode = ?"
        ).format(self.STATE_TABLE)
        self._execute(
            cmd,
            (
                actual_mtime,
                actual_size,
                checksum,
                current_timestamp(),
                self._to_sqlite(actual_inode),
            ),
        )

    def _insert_new_state_record(
        self, actual_inode, actual_mtime, actual_size, checksum
    ):
        assert checksum is not None

        cmd = (
            "INSERT INTO {}(inode, mtime, size, md5, timestamp) "
            "VALUES (?, ?, ?, ?, ?)"
        ).format(self.STATE_TABLE)
        self._execute(
            cmd,
            (
                self._to_sqlite(actual_inode),
                actual_mtime,
                actual_size,
                checksum,
                current_timestamp(),
            ),
        )
        self.inserts += 1

    def get_state_record_for_inode(self, inode):
        cmd = (
            "SELECT mtime, size, md5, timestamp from {} WHERE "
            "inode=?".format(self.STATE_TABLE)
        )
        self._execute(cmd, (self._to_sqlite(inode),))
        results = self._fetchall()
        if results:
            # uniqueness constrain on inode
            assert len(results) == 1
            return results[0]
        return None

    def save(self, path_info, hash_info):
        """Save hash for the specified path info.

        Args:
            path_info (dict): path_info to save hash for.
            hash_info (HashInfo): hash to save.
        """
        assert isinstance(path_info, str) or path_info.scheme == "local"
        assert hash_info
        assert isinstance(hash_info, HashInfo)
        assert os.path.exists(path_info)

        actual_mtime, actual_size = get_mtime_and_size(path_info, self.tree)
        actual_inode = get_inode(path_info)

        existing_record = self.get_state_record_for_inode(actual_inode)
        if not existing_record:
            self._insert_new_state_record(
                actual_inode, actual_mtime, actual_size, hash_info.value
            )
            return

        self._update_state_for_path_changed(
            actual_inode, actual_mtime, actual_size, hash_info.value
        )

    def get(self, path_info):
        """Gets the hash for the specified path info. Hash will be
        retrieved from the state database if available.

        Args:
            path_info (dict): path info to get the hash for.

        Returns:
            HashInfo or None: hash for the specified path info or None if it
            doesn't exist in the state database.
        """
        assert isinstance(path_info, str) or path_info.scheme == "local"
        path = os.fspath(path_info)

        # NOTE: use os.path.exists instead of LocalTree.exists
        # because it uses lexists() and will return True for broken
        # symlinks that we cannot stat() in get_mtime_and_size
        if not os.path.exists(path):
            return None

        actual_mtime, actual_size = get_mtime_and_size(path, self.tree)
        actual_inode = get_inode(path)

        existing_record = self.get_state_record_for_inode(actual_inode)
        if not existing_record:
            return None

        mtime, size, value, _ = existing_record
        if self._file_metadata_changed(actual_mtime, mtime, actual_size, size):
            return None

        self._update_state_record_timestamp_for_inode(actual_inode)
        return HashInfo("md5", value, size=int(actual_size))

    def save_link(self, path_info):
        """Adds the specified path to the list of links created by dvc. This
        list is later used on `dvc checkout` to cleanup old links.

        Args:
            path_info (dict): path info to add to the list of links.
        """
        assert isinstance(path_info, str) or path_info.scheme == "local"

        if not self.tree.exists(path_info):
            return

        mtime, _ = get_mtime_and_size(path_info, self.tree)
        inode = get_inode(path_info)
        relative_path = relpath(path_info, self.root_dir)

        cmd = "REPLACE INTO {}(path, inode, mtime) " "VALUES (?, ?, ?)".format(
            self.LINK_STATE_TABLE
        )
        self._execute(cmd, (relative_path, self._to_sqlite(inode), mtime))

    def get_unused_links(self, used):
        """Removes all saved links except the ones that are used.

        Args:
            used (list): list of used links that should not be removed.
        """
        unused = []

        self._execute(f"SELECT * FROM {self.LINK_STATE_TABLE}")
        for row in self.cursor:
            relative_path, inode, mtime = row
            inode = self._from_sqlite(inode)
            path = os.path.join(self.root_dir, relative_path)

            if path in used or not self.tree.exists(path):
                continue

            actual_inode = get_inode(path)
            actual_mtime, _ = get_mtime_and_size(path, self.tree)

            if (inode, mtime) == (actual_inode, actual_mtime):
                logger.debug("Removing '%s' as unused link.", path)
                unused.append(relative_path)

        return unused

    def remove_links(self, unused):
        for path in unused:
            remove(path)

        for chunk_unused in to_chunks(
            unused, chunk_size=SQLITE_MAX_VARIABLES_NUMBER
        ):
            cmd = "DELETE FROM {} WHERE path IN ({})".format(
                self.LINK_STATE_TABLE, ",".join(["?"] * len(chunk_unused))
            )
            self._execute(cmd, tuple(chunk_unused))


def _connect_sqlite(filename, options):
    # Connect by URI was added in Python 3.4 and sqlite 3.7.7,
    # we ignore options, which should be fine unless repo is on old NFS/CIFS
    if sqlite3.sqlite_version_info < (3, 7, 7):
        return sqlite3.connect(filename)

    uri = _build_sqlite_uri(filename, options)
    return sqlite3.connect(uri, uri=True)


def _build_sqlite_uri(filename, options):
    # In the doc mentioned below we only need to replace ? -> %3f and
    # # -> %23, but, if present, we also need to replace % -> %25 first
    # (happens when we are on a weird FS that shows urlencoded filenames
    # instead of proper ones) to not confuse sqlite.
    uri_path = filename.replace("%", "%25")

    # Convert filename to uri according to https://www.sqlite.org/uri.html, 3.1
    uri_path = uri_path.replace("?", "%3f").replace("#", "%23")
    if os.name == "nt":
        uri_path = uri_path.replace("\\", "/")
        uri_path = re.sub(r"^([a-z]:)", "/\\1", uri_path, flags=re.I)
    uri_path = re.sub(r"/+", "/", uri_path)

    # Empty netloc, params and fragment
    return urlunparse(("file", "", uri_path, "", urlencode(options), ""))
