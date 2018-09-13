import os
import sqlite3
import nanotime

from dvc.system import System
from dvc.utils import file_md5, remove
from dvc.exceptions import DvcException
from dvc.logger import Logger


class StateDuplicateError(DvcException):
    pass


class State(object):
    STATE_FILE = 'state'
    STATE_LOCK_FILE = 'state.lock'
    STATE_TABLE = 'state'
    STATE_TABLE_LAYOUT = "inode TEXT PRIMARY KEY, " \
                         "mtime TEXT NOT NULL, " \
                         "md5 TEXT NOT NULL"

    def __init__(self, project):
        self.project = project
        self.dvc_dir = project.dvc_dir

        if not self.dvc_dir:
            self.state_file = None
            return

        self.state_file = os.path.join(self.dvc_dir, self.STATE_FILE)
        # Try loading once to check that the file is indeed a database
        # and reformat it if it is not.
        try:
            db = self.load()
            db.close()
        except sqlite3.DatabaseError:
            os.unlink(self.state_file)
            db = self.load()
            db.close()

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

        if not md5 or not actual:
            return True

        return actual.split('.')[0] != md5.split('.')[0]

    def load(self):
        db = sqlite3.connect(self.state_file)
        c = db.cursor()
        # Check that the state file is indeed a database
        cmd = "CREATE TABLE IF NOT EXISTS {} ({})"
        c.execute(cmd.format(self.STATE_TABLE, self.STATE_TABLE_LAYOUT))
        c.execute("SELECT name FROM sqlite_master WHERE type='table';")
        db.commit()
        c.close()
        return db

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
        return str(System.inode(path))

    def _do_update(self, path, use_db=None):
        if not os.path.exists(path):
            return (None, None)

        mtime = self.mtime(path)
        inode = self.inode(path)

        if use_db is None:
            db = self.load()
        else:
            db = use_db

        c = db.cursor()

        md5 = self._get(inode, mtime, use_db=db)
        if md5:
            return (md5, None)

        md5, info = self._collect(path)

        cmd = 'REPLACE INTO {}(inode, mtime, md5) ' \
              'VALUES ("{}", "{}", "{}")'.format(self.STATE_TABLE,
                                                 inode,
                                                 mtime,
                                                 md5)

        c.execute(cmd)
        c.close()
        if use_db is None:
            db.commit()
            db.close()

        return (md5, info)

    def update(self, path, use_db=None):
        return self._do_update(path, use_db=use_db)[0]

    def update_info(self, path, use_db=None):
        md5, info = self._do_update(path, use_db=use_db)
        if not info:
            info = self.project.cache.local.load_dir_cache(md5)
        return (md5, info)

    def _get(self, inode, mtime, use_db=None):
        cmd = 'SELECT * from {} WHERE inode="{}"'.format(self.STATE_TABLE,
                                                         inode)

        if use_db is None:
            db = self.load()
        else:
            db = use_db

        c = db.cursor()
        c.execute(cmd)
        ret = c.fetchall()
        c.close()

        if use_db is None:
            db.commit()
            db.close()

        if len(ret) == 0:
            return None
        assert len(ret) == 1
        assert len(ret[0]) == 3
        i, m, md5 = ret[0]
        assert i == inode

        if mtime == m:
            return md5

        return None

    def get(self, path):
        mtime = self.mtime(path)
        inode = self.inode(path)

        return self._get(inode, mtime)


class LinkState(State):
    STATE_FILE = 'link.state'
    STATE_LOCK_FILE = STATE_FILE + '.lock'
    STATE_TABLE = 'link'
    STATE_TABLE_LAYOUT = "path TEXT PRIMARY KEY, " \
                         "inode TEXT NOT NULL, " \
                         "mtime TEXT NOT NULL"

    def __init__(self, project):
        super(LinkState, self).__init__(project)
        self.root_dir = project.root_dir

    def update(self, path, use_db=None):
        if not os.path.exists(path):
            return

        mtime = self.mtime(path)
        inode = self.inode(path)
        relpath = os.path.relpath(path, self.root_dir)

        if use_db is None:
            db = self.load()
        else:
            db = use_db

        c = db.cursor()

        cmd = 'REPLACE INTO {}(path, inode, mtime) ' \
              'VALUES ("{}", "{}", "{}")'.format(self.STATE_TABLE,
                                                 relpath,
                                                 inode,
                                                 mtime)
        c.execute(cmd)
        c.close()

        if use_db is None:
            db.commit()
            db.close()

    def remove_unused(self, used):
        unused = []

        db = self.load()
        c = db.cursor()
        c.execute('SELECT * FROM {}'.format(self.STATE_TABLE))
        for row in c:
            p, i, m = row
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

        db.commit()
        for p in unused:
            cmd = 'DELETE FROM {} WHERE path = "{}"'
            c.execute(cmd.format(self.STATE_TABLE, p))

        c.close()
        db.commit()
        db.close()
