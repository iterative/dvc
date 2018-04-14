import os
import json
import nanotime
import threading

from dvc.system import System
from dvc.output import Output
from dvc.utils import file_md5, dict_md5
from dvc.exceptions import DvcException


class StateEntry(object):
    PARAM_MTIME = 'mtime'
    PARAM_MD5 = 'md5'

    def __init__(self, md5, mtime):
        self.mtime = mtime
        self.md5 = md5

    def update(self, md5, mtime):
        self.mtime = mtime
        self.md5 = md5

    @staticmethod
    def loadd(d):
        mtime = d[StateEntry.PARAM_MTIME]
        md5 = d[StateEntry.PARAM_MD5]
        return StateEntry(md5, mtime)

    def dumpd(self):
        return {
            self.PARAM_MD5: self.md5,
            self.PARAM_MTIME: self.mtime,
        }


class StateDuplicateError(DvcException):
    pass


class State(object):
    STATE_FILE = 'state'

    def __init__(self, root_dir, dvc_dir):
        self.root_dir = root_dir
        self.dvc_dir = dvc_dir
        self.state_file = os.path.join(dvc_dir, self.STATE_FILE)
        self._db = self.load()
        self._lock = threading.Lock()

    @staticmethod
    def init(root_dir, dvc_dir):
        return State(root_dir, dvc_dir)

    def collect_dir(self, dname):
        dir_info = []

        for root, dirs, files in os.walk(dname):
            for fname in files:
                path = os.path.join(root, fname)
                relpath = os.path.relpath(path, dname)

                md5 = self.update(path, dump=False)
                dir_info.append({Output.PARAM_RELPATH: relpath, Output.PARAM_MD5: md5})

        self.dump()

        return dir_info

    def compute_md5(self, path):
        if os.path.isdir(path):
            dir_info = self.collect_dir(path)
            return dict_md5(dir_info) + Output.MD5_DIR_SUFFIX
        else:
            return file_md5(path)[0]

    def changed(self, path, md5):
        actual = self.update(path)

        if not md5 or not actual:
            return True

        return actual.split('.')[0] != md5.split('.')[0]

    def load(self):
        if not os.path.isfile(self.state_file):
            return {}

        with open(self.state_file, 'r') as fd:
            return json.load(fd)

    def dump(self):
        with open(self.state_file, 'w+') as fd:
            json.dump(self._db, fd)

    @staticmethod
    def mtime(path):
        return str(int(nanotime.timestamp(os.path.getmtime(path))))

    @staticmethod
    def inode(path):
        return str(System.inode(path))

    def update(self, path, dump=True):
        if not os.path.exists(path):
            return None

        mtime = self.mtime(path)
        inode = self.inode(path)

        md5 = self._get(inode, mtime)
        if md5:
            return md5

        md5 = self.compute_md5(path)
        state = StateEntry(md5, mtime)
        d = state.dumpd()

        with self._lock:
            self._db[inode] = d

            if dump:
                self.dump()

        return md5

    def _get(self, inode, mtime):
        with self._lock:
            d = self._db.get(inode, None)
        if not d:
            return None

        state = StateEntry.loadd(d)
        if mtime == state.mtime:
            return state.md5

        return None

    def get(self, path):
        mtime = self.mtime(path)
        inode = self.inode(path)

        return self._get(inode, mtime)
