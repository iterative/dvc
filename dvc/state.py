"""Manages state database used for checksum caching."""

from __future__ import unicode_literals

import os
import sqlite3

import dvc.logger as logger
from dvc.config import Config
from dvc.utils import file_md5, remove, current_timestamp
from dvc.exceptions import DvcException
from dvc.utils.fs import get_mtime_and_size, get_inode


class StateVersionTooNewError(DvcException):
    """Thrown when dvc version is older than the state database version."""

    def __init__(self, dvc_version, expected, actual):
        super(StateVersionTooNewError, self).__init__(
            "you are using an old version '{dvc_version}' of dvc that is "
            "using state file version '{expected}' which is not compatible "
            "with the state file version '{actual}' that is used in this "
            "repo. Please upgrade right now!".format(
                dvc_version=dvc_version, expected=expected, actual=actual
            )
        )


def _file_metadata_changed(actual_mtime, mtime, actual_size, size):
    return actual_mtime != mtime or actual_size != size


class State(object):  # pylint: disable=too-many-instance-attributes
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

    def __init__(self, repo, config):
        self.repo = repo
        self.dvc_dir = repo.dvc_dir
        self.root_dir = repo.root_dir

        self.row_limit = 100
        self.row_cleanup_quota = 50

        state_config = config.get(Config.SECTION_STATE, {})
        self.row_limit = state_config.get(
            Config.SECTION_STATE_ROW_LIMIT, self.STATE_ROW_LIMIT
        )
        self.row_cleanup_quota = state_config.get(
            Config.SECTION_STATE_ROW_CLEANUP_QUOTA,
            self.STATE_ROW_CLEANUP_QUOTA,
        )

        if not self.dvc_dir:
            self.state_file = None
            return

        self.state_file = os.path.join(self.dvc_dir, self.STATE_FILE)

        # https://www.sqlite.org/tempfiles.html
        self.temp_files = [
            self.state_file + "-journal",
            self.state_file + "-wal",
        ]

        self.database = None
        self.cursor = None
        self.inserts = 0

    def __enter__(self):
        self.load()

    def __exit__(self, typ, value, tbck):
        self.dump()

    def _collect(self, path):
        if os.path.isdir(path):
            return self.repo.cache.local.collect_dir_cache(path)
        return (file_md5(path)[0], None)

    def changed(self, path, md5):
        """Check if file/directory has the expected md5.

        Args:
            path (str): path to the file/directory to check.
            md5 (str): expected md5.

        Returns:
            bool: True if path has the expected md5, False otherwise.
        """
        actual = self.update(path)

        msg = "File '{}', md5 '{}', actual '{}'"
        logger.debug(msg.format(path, md5, actual))

        if not md5 or not actual:
            return True

        return actual.split(".")[0] != md5.split(".")[0]

    def _execute(self, cmd):
        logger.debug(cmd)
        return self.cursor.execute(cmd)

    def _fetchall(self):
        ret = self.cursor.fetchall()
        logger.debug("fetched: {}".format(ret))
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
        from dvc import VERSION

        if not empty:
            cmd = "PRAGMA user_version;"
            self._execute(cmd)
            ret = self._fetchall()
            assert len(ret) == 1
            assert len(ret[0]) == 1
            assert isinstance(ret[0][0], int)
            version = ret[0][0]

            if version > self.VERSION:
                raise StateVersionTooNewError(VERSION, self.VERSION, version)
            elif version < self.VERSION:
                msg = (
                    "State file version '{}' is too old. "
                    "Reformatting to the current version '{}'."
                )
                logger.warning(msg.format(version, self.VERSION))
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
            self.database = sqlite3.connect(self.state_file)
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

        cmd = "SELECT count from {} WHERE rowid={}"
        self._execute(cmd.format(self.STATE_INFO_TABLE, self.STATE_INFO_ROW))
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

        cmd = "UPDATE {} SET count = {} WHERE rowid = {}"
        self._execute(
            cmd.format(
                self.STATE_INFO_TABLE,
                self._to_sqlite(count),
                self.STATE_INFO_ROW,
            )
        )

        self._update_cache_directory_state()

        self.database.commit()
        self.cursor.close()
        self.database.close()
        self.database = None
        self.cursor = None
        self.inserts = 0

    def _do_update(self, path, known_checksum=None):
        """
        Make sure the stored info for the given path is up to date.
        """
        if not os.path.exists(path):
            return None, None

        actual_mtime, actual_size = get_mtime_and_size(path)
        actual_inode = get_inode(path)

        existing_record = self.get_state_record_for_inode(actual_inode)

        if existing_record:
            md5, info = self._update_existing_state_record(
                path,
                actual_inode,
                actual_mtime,
                actual_size,
                existing_record,
                known_checksum,
            )
        else:
            md5, info = self._insert_new_state_record(
                path, actual_inode, actual_mtime, actual_size, known_checksum
            )

        return md5, info

    def _update_existing_state_record(
        self,
        path,
        actual_inode,
        actual_mtime,
        actual_size,
        existing_record,
        known_checksum=None,
    ):

        mtime, size, md5, _ = existing_record
        if _file_metadata_changed(actual_mtime, mtime, actual_size, size):
            md5, info = self._update_state_for_path_changed(
                path, actual_inode, actual_mtime, actual_size, known_checksum
            )
        else:
            info = None
            self._update_state_record_timestamp_for_inode(actual_inode)
        return md5, info

    def _update_state_record_timestamp_for_inode(self, actual_inode):
        cmd = 'UPDATE {} SET timestamp = "{}" WHERE inode = {}'
        self._execute(
            cmd.format(
                self.STATE_TABLE,
                current_timestamp(),
                self._to_sqlite(actual_inode),
            )
        )

    def _update_state_for_path_changed(
        self,
        path,
        actual_inode,
        actual_mtime,
        actual_size,
        known_checksum=None,
    ):
        if known_checksum:
            md5, info = known_checksum, None
        else:
            md5, info = self._collect(path)
        cmd = (
            "UPDATE {} SET "
            'mtime = "{}", size = "{}", '
            'md5 = "{}", timestamp = "{}" '
            "WHERE inode = {}"
        )
        self._execute(
            cmd.format(
                self.STATE_TABLE,
                actual_mtime,
                actual_size,
                md5,
                current_timestamp(),
                self._to_sqlite(actual_inode),
            )
        )
        return md5, info

    def _insert_new_state_record(
        self, path, actual_inode, actual_mtime, actual_size, known_checksum
    ):
        if known_checksum:
            md5, info = known_checksum, None
        else:
            md5, info = self._collect(path)
        cmd = (
            "INSERT INTO {}(inode, mtime, size, md5, timestamp) "
            'VALUES ({}, "{}", "{}", "{}", "{}")'
        )
        self._execute(
            cmd.format(
                self.STATE_TABLE,
                self._to_sqlite(actual_inode),
                actual_mtime,
                actual_size,
                md5,
                current_timestamp(),
            )
        )
        self.inserts += 1
        return md5, info

    def get_state_record_for_inode(self, inode):
        cmd = "SELECT mtime, size, md5, timestamp from {} " "WHERE inode={}"
        cmd = cmd.format(self.STATE_TABLE, self._to_sqlite(inode))
        self._execute(cmd)
        results = self._fetchall()
        if results:
            # uniquness constrain on inode
            assert len(results) == 1
            return results[0]
        return None

    def update(self, path, known_checksum=None):
        """Gets the checksum for the specified path. Checksum will be
        retrieved from the state database if available, otherwise it will be
        computed and cached in the state database for the further use.

        Args:
            path (str): path to get the checksum for.

        Returns:
            str: checksum for the specified path.
        """
        return self._do_update(path, known_checksum)[0]

    def update_info(self, path):
        """Gets the checksum and the directory info (if applicable) for the
        specified path.

        Args:
            path (str): path to get the checksum and the directory info for.

        Returns:
            tuple: checksum for the specified path along with a directory info
            (list of {relative_path: checksum} entries for each file in the
            directory) if applicable, otherwise None.
        """
        md5, info = self._do_update(path)
        if not info:
            info = self.repo.cache.local.load_dir_cache(md5)
        return (md5, info)

    def update_link(self, path):
        """Adds the specified path to the list of links created by dvc. This
        list is later used on `dvc checkout` to cleanup old links.

        Args:
            path (str): path to add to the list of links.
        """
        if not os.path.exists(path):
            return

        mtime, _ = get_mtime_and_size(path)
        inode = get_inode(path)
        relpath = os.path.relpath(path, self.root_dir)

        cmd = (
            "REPLACE INTO {}(path, inode, mtime) "
            'VALUES ("{}", {}, "{}")'.format(
                self.LINK_STATE_TABLE, relpath, self._to_sqlite(inode), mtime
            )
        )
        self._execute(cmd)

    def remove_unused_links(self, used):
        """Removes all saved links except the ones that are used.

        Args:
            used (list): list of used links that should not be removed.
        """
        unused = []

        self._execute("SELECT * FROM {}".format(self.LINK_STATE_TABLE))
        for row in self.cursor:
            relpath, inode, mtime = row
            inode = self._from_sqlite(inode)
            path = os.path.join(self.root_dir, relpath)

            if path in used:
                continue

            if not os.path.exists(path):
                continue

            actual_inode = get_inode(path)
            actual_mtime, _ = get_mtime_and_size(path)

            if inode == actual_inode and mtime == actual_mtime:
                logger.debug("Removing '{}' as unused link.".format(path))
                remove(path)
                unused.append(relpath)

        for relpath in unused:
            cmd = 'DELETE FROM {} WHERE path = "{}"'
            self._execute(cmd.format(self.LINK_STATE_TABLE, relpath))

    def _update_cache_directory_state(self):
        cache_path = self.repo.cache.local.cache_dir
        mtime, size = get_mtime_and_size(cache_path)
        inode = get_inode(cache_path)

        cmd = (
            "INSERT OR REPLACE INTO {}(inode, size, mtime, timestamp, md5) "
            'VALUES ({}, "{}", "{}", "{}", "")'.format(
                self.STATE_TABLE,
                self._to_sqlite(inode),
                size,
                mtime,
                current_timestamp(),
            )
        )
        self._execute(cmd)
