import os
import tinydb
from checksumdir import dirhash

from dvc.system import System
from dvc.output import Output
from dvc.utils import file_md5
from dvc.exceptions import DvcException


class StateEntry(object):
    PARAM_MTIME = 'mtime'
    PARAM_MD5 = 'md5'
    PARAM_INODE = 'inode'

    def __init__(self, md5, mtime, inode):
        self.mtime = mtime
        self.md5 = md5
        self.inode = inode

    def update(self, md5, mtime, inode):
        self.mtime = mtime
        self.md5 = md5
        self.inode = inode

    @staticmethod
    def loadd(d):
        mtime = d[StateEntry.PARAM_MTIME]
        md5 = d[StateEntry.PARAM_MD5]
        inode = d[StateEntry.PARAM_INODE]
        return StateEntry(md5, mtime, inode)

    def dumpd(self):
        return {
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

    def compute_md5(self, path):
        if os.path.isdir(path):
            return dirhash(path, hashfunc='md5') + Output.MD5_DIR_SUFFIX
        else:
            return file_md5(path)[0]

    def changed(self, path, md5):
        state = self.update(path)
        return state.md5 != md5

    def update(self, path):
        mtime = os.path.getmtime(path)
        inode = System.inode(path)

        state = self._get(inode, mtime)
        if state:
            return state

        md5 = self.compute_md5(path)
        state = StateEntry(md5, mtime, inode)
        d = state.dumpd()
        if self._db.contains(self._q.inode == inode):
            self._db.update(d, self._q.inode == inode)
        else:
            self._db.insert(d)

        return state

    def _get(self, inode, mtime):
        d_list = self._db.search(self._q.inode == inode)
        if not len(d_list):
            return None

        if len(d_list) > 1:
            raise StateDuplicateError()

        state = StateEntry.loadd(d_list[0])
        if mtime == state.mtime and inode == state.inode:
            return state

        return None

    def get(self, path):
        mtime = os.path.getmtime(path)
        inode = System.inode(path)

        return self._get(inode, mtime)
