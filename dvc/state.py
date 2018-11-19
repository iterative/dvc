import os
import time
import sqlite3
import nanotime

from dvc.config import Config
from dvc.system import System
from dvc.utils import file_md5, remove
from dvc.exceptions import DvcException
from dvc.logger import Logger


class StateDuplicateError(DvcException):
    pass


class State(object):
    VERSION = 2
    STATE_FILE = 'state'
    STATE_TABLE = 'state'
    STATE_TABLE_LAYOUT = "inode INTEGER PRIMARY KEY, " \
                         "mtime TEXT NOT NULL, " \
                         "md5 TEXT NOT NULL, " \
                         "timestamp TEXT NOT NULL"

    STATE_INFO_TABLE = 'state_info'
    STATE_INFO_TABLE_LAYOUT = 'count INTEGER'
    STATE_INFO_ROW = 1

    LINK_STATE_TABLE = 'link_state'
    LINK_STATE_TABLE_LAYOUT = "path TEXT PRIMARY KEY, " \
                              "inode INTEGER NOT NULL, " \
                              "mtime TEXT NOT NULL"

    STATE_ROW_LIMIT = 100000000
    STATE_ROW_CLEANUP_QUOTA = 50

    MAX_INT = 2**63 - 1
    MAX_UINT = 2**64 - 2

    def __init__(self, project, config):
        self.project = project
        self.dvc_dir = project.dvc_dir
        self.root_dir = project.root_dir

        self.row_limit = 100
        self.row_cleanup_quota = 50

        c = config.get(Config.SECTION_STATE, {})
        self.row_limit = c.get(Config.SECTION_STATE_ROW_LIMIT,
                               self.STATE_ROW_LIMIT)
        self.row_cleanup_quota = c.get(Config.SECTION_STATE_ROW_CLEANUP_QUOTA,
                                       self.STATE_ROW_CLEANUP_QUOTA)

        if not self.dvc_dir:
            self.state_file = None
            return

        self.state_file = os.path.join(self.dvc_dir, self.STATE_FILE)

        # https://www.sqlite.org/tempfiles.html
        self.temp_files = [
            self.state_file + '-journal',
            self.state_file + '-wal',
        ]

        self.db = None
        self.c = None
        self.inserts = 0

    def __enter__(self):
        self.load()

    def __exit__(self, type, value, tb):
        self.dump()

    @staticmethod
    def init(project):
        return State(project)

    def _collect(self, path):
        if os.path.isdir(path):
            return self.project.cache.local.collect_dir_cache(path)
        else:
            return (file_md5(path)[0], None)

    def changed(self, path, md5):
        actual = self.update(path)

        msg = "File '{}', md5 '{}', actual '{}'"
        Logger.debug(msg.format(path, md5, actual))

        if not md5 or not actual:
            return True

        return actual.split('.')[0] != md5.split('.')[0]

    def _execute(self, cmd):
        Logger.debug(cmd)
        return self.c.execute(cmd)

    def _fetchall(self):
        ret = self.c.fetchall()
        Logger.debug("fetched: {}".format(ret))
        return ret

    def _to_sqlite(self, n):
        assert n >= 0
        assert n < self.MAX_UINT
        # NOTE: sqlite stores unit as signed ints, so maximum uint is 2^63-1
        # see http://jakegoulding.com/blog/2011/02/06/sqlite-64-bit-integers/
        if n > self.MAX_INT:
            ret = -(n - self.MAX_INT)
        else:
            ret = n
        assert self._from_sqlite(ret) == n
        return ret

    def _from_sqlite(self, n):
        assert abs(n) <= self.MAX_INT
        if n < 0:
            return abs(n) + self.MAX_INT
        assert n < self.MAX_UINT
        assert n >= 0
        return n

    def _load(self, empty=False):
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
                msg = "You are using an old version '{}' of dvc that is " \
                      "using state file version '{}' which is not " \
                      "compatible with the state file version '{}' that " \
                      "is used in this projet. Please upgrade right now!"
                raise DvcException(msg.format(VERSION,
                                              self.VERSION,
                                              version))
            elif version < self.VERSION:
                msg = "State file version '{}' is too old. " \
                      "Reformatting to the current version '{}'."
                self.project.logger.warn(msg.format(version, self.VERSION))
                cmd = "DROP TABLE IF EXISTS {};"
                self._execute(cmd.format(self.STATE_TABLE))
                self._execute(cmd.format(self.STATE_INFO_TABLE))
                self._execute(cmd.format(self.LINK_STATE_TABLE))

        # Check that the state file is indeed a database
        cmd = "CREATE TABLE IF NOT EXISTS {} ({})"
        self._execute(cmd.format(self.STATE_TABLE,
                                 self.STATE_TABLE_LAYOUT))
        self._execute(cmd.format(self.STATE_INFO_TABLE,
                                 self.STATE_INFO_TABLE_LAYOUT))
        self._execute(cmd.format(self.LINK_STATE_TABLE,
                                 self.LINK_STATE_TABLE_LAYOUT))

        cmd = "INSERT OR IGNORE INTO {} (count) SELECT 0 " \
              "WHERE NOT EXISTS (SELECT * FROM {})"
        self._execute(cmd.format(self.STATE_INFO_TABLE,
                                 self.STATE_INFO_TABLE))

        cmd = "PRAGMA user_version = {};"
        self._execute(cmd.format(self.VERSION))

    def load(self):
        retries = 1
        while True:
            assert self.db is None
            assert self.c is None
            assert self.inserts == 0
            empty = not os.path.exists(self.state_file)
            self.db = sqlite3.connect(self.state_file)
            self.c = self.db.cursor()

            # Try loading once to check that the file is indeed a database
            # and reformat it if it is not.
            try:
                self._load(empty=empty)
                return
            except sqlite3.DatabaseError:
                self.c.close()
                self.db.close()
                self.db = None
                self.c = None
                self.inserts = 0
                if retries > 0:
                    os.unlink(self.state_file)
                    retries -= 1
                else:
                    raise

    def _vacuum(self):
        # NOTE: see https://bugs.python.org/issue28518
        self.db.isolation_level = None
        self._execute("VACUUM")
        self.db.isolation_level = ''

    def dump(self):
        assert self.db is not None

        cmd = "SELECT count from {} WHERE rowid={}"
        self._execute(cmd.format(self.STATE_INFO_TABLE,
                                 self.STATE_INFO_ROW))
        ret = self._fetchall()
        assert len(ret) == 1
        assert len(ret[0]) == 1
        count = self._from_sqlite(ret[0][0]) + self.inserts

        if count > self.row_limit:
            msg = "Cleaning up state. This might take a while."
            self.project.logger.warn(msg)

            delete = (count - self.row_limit)
            delete += int(self.row_limit * (self.row_cleanup_quota/100.))
            cmd = "DELETE FROM {} WHERE timestamp IN (" \
                  "SELECT timestamp FROM {} ORDER BY timestamp ASC LIMIT {});"
            self._execute(cmd.format(self.STATE_TABLE,
                                     self.STATE_TABLE,
                                     delete))

            self._vacuum()

            cmd = "SELECT COUNT(*) FROM {}"

            self._execute(cmd.format(self.STATE_TABLE))
            ret = self._fetchall()
            assert len(ret) == 1
            assert len(ret[0]) == 1
            count = ret[0][0]

        cmd = "UPDATE {} SET count = {} WHERE rowid = {}"
        self._execute(cmd.format(self.STATE_INFO_TABLE,
                                 self._to_sqlite(count),
                                 self.STATE_INFO_ROW))

        self.db.commit()
        self.c.close()
        self.db.close()
        self.db = None
        self.c = None
        self.inserts = 0

    @staticmethod
    def mtime(path):
        mtime = os.path.getmtime(path)
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for entry in list(dirs) + list(files):
                    p = os.path.join(root, entry)
                    m = os.path.getmtime(p)
                    if m > mtime:
                        mtime = m

        return str(int(nanotime.timestamp(mtime)))

    @staticmethod
    def inode(path):
        Logger.debug('Path {} inode {}'.format(path, System.inode(path)))
        return System.inode(path)

    def _do_update(self, path):
        """
        Search for the current inode on the table or update it with
        gathered information.
        """
        if not os.path.exists(path):
            return (None, None)

        mtime = self.mtime(path)
        inode = self.inode(path)

        cmd = 'SELECT * from {} WHERE inode={}'.format(self.STATE_TABLE,
                                                       self._to_sqlite(inode))

        self._execute(cmd)
        ret = self._fetchall()
        if len(ret) == 0:
            md5, info = self._collect(path)
            cmd = 'INSERT INTO {}(inode, mtime, md5, timestamp) ' \
                  'VALUES ({}, "{}", "{}", "{}")'
            self._execute(cmd.format(self.STATE_TABLE,
                                     self._to_sqlite(inode),
                                     mtime,
                                     md5,
                                     int(nanotime.timestamp(time.time()))))
            self.inserts += 1
        else:
            assert len(ret) == 1
            assert len(ret[0]) == 4
            i, m, md5, timestamp = ret[0]
            i = self._from_sqlite(i)
            assert i == inode
            msg = "Inode '{}', mtime '{}', actual mtime '{}'."
            Logger.debug(msg.format(i, m, mtime))
            if mtime != m:
                md5, info = self._collect(path)
                cmd = 'UPDATE {} SET ' \
                      'mtime = "{}", md5 = "{}", timestamp = "{}" ' \
                      'WHERE inode = {}'
                self._execute(cmd.format(self.STATE_TABLE,
                                         mtime,
                                         md5,
                                         int(nanotime.timestamp(time.time())),
                                         self._to_sqlite(inode)))
            else:
                info = None
                cmd = 'UPDATE {} SET timestamp = "{}" WHERE inode = {}'
                self._execute(cmd.format(self.STATE_TABLE,
                                         int(nanotime.timestamp(time.time())),
                                         self._to_sqlite(inode)))

        return (md5, info)

    def update(self, path):
        return self._do_update(path)[0]

    def update_info(self, path):
        md5, info = self._do_update(path)
        if not info:
            info = self.project.cache.local.load_dir_cache(md5)
        return (md5, info)

    def update_link(self, path):
        if not os.path.exists(path):
            return

        mtime = self.mtime(path)
        inode = self.inode(path)
        relpath = os.path.relpath(path, self.root_dir)

        cmd = 'REPLACE INTO {}(path, inode, mtime) ' \
              'VALUES ("{}", {}, "{}")'.format(self.LINK_STATE_TABLE,
                                               relpath,
                                               self._to_sqlite(inode),
                                               mtime)
        self._execute(cmd)

    def remove_unused_links(self, used):
        unused = []

        self._execute('SELECT * FROM {}'.format(self.LINK_STATE_TABLE))
        for row in self.c:
            p, i, m = row
            i = self._from_sqlite(i)
            path = os.path.join(self.root_dir, p)

            if path in used:
                continue

            if not os.path.exists(path):
                continue

            inode = self.inode(path)
            mtime = self.mtime(path)

            if i == inode and m == mtime:
                Logger.debug('Removing \'{}\' as unused link.'.format(path))
                remove(path)
                unused.append(p)

        for p in unused:
            cmd = 'DELETE FROM {} WHERE path = "{}"'
            self._execute(cmd.format(self.LINK_STATE_TABLE, p))
