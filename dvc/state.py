import os
import tinydb

from dvc.exceptions import DvcException


class StateEntry(object):
    PARAM_PATH = 'path'
    PARAM_MTIME = 'mtime'
    PARAM_MD5 = 'md5'
    PARAM_INODE = 'inode'

    def __init__(self, root_dir, path, md5, mtime, inode):
        self.root_dir = root_dir
        self.path = path
        self.mtime = mtime
        self.md5 = md5
        self.inode = inode
        self.dvc_path = os.path.relpath(self.path, self.root_dir)

    def update(self, md5, mtime, inode):
        self.mtime = mtime
        self.md5 = md5
        self.inode = inode

    @staticmethod
    def loadd(root_dir, d):
        path = os.path.join(root_dir, d[StateEntry.PARAM_PATH])
        mtime = d[StateEntry.PARAM_MTIME]
        md5 = d[StateEntry.PARAM_MD5]
        inode = d[StateEntry.PARAM_INODE]
        return StateEntry(root_dir, path, md5, mtime, inode)

    def dumpd(self):
        return {
            self.PARAM_PATH: self.dvc_path,
            self.PARAM_MD5: self.md5,
            self.PARAM_MTIME: self.mtime,
            self.PARAM_INODE: self.inode
        }


class StateDuplicateError(DvcException):
    pass


class State(object):
    STATE_FILE = 'state'

    def __init__(self, root_dir, dvc_dir):
        self.root_dir = root_dir
        self.dvc_dir = dvc_dir
        self.state_file = os.path.join(dvc_dir, self.STATE_FILE)
        self._db = tinydb.TinyDB(self.state_file)
        self._q = tinydb.Query()

    @staticmethod
    def init(root_dir, dvc_dir):
        return State(root_dir, dvc_dir)

    def update(self, path, md5, mtime, inode):
        existing = self.get(path)
        if not existing:
            return self.add(path, md5, mtime, inode)

        state = StateEntry(self.root_dir, path, md5, mtime, inode)
        self._db.update(state.dumpd(), self._q.path == state.dvc_path)

        return state

    def add(self, path, md5, mtime, inode):
        entry = StateEntry(self.root_dir, path, md5, mtime, inode)
        self._db.insert(entry.dumpd())
        return entry

    def get(self, path):
        d_list = self._db.search(self._q.path == os.path.relpath(path, self.root_dir))
        if not len(d_list):
            return None

        if len(d_list) > 1:
            raise StateDuplicateError()

        return StateEntry.loadd(self.root_dir, d_list[0])
