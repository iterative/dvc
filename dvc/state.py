import os
import json
import nanotime
import threading

from dvc.lock import Lock
from dvc.system import System
from dvc.utils import file_md5, remove
from dvc.exceptions import DvcException
from dvc.signal_handler import SignalHandler
from dvc.logger import Logger


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
    STATE_LOCK_FILE = 'state.lock'

    def __init__(self, project):
        self.project = project
        self.dvc_dir = project.dvc_dir
        self._lock = threading.Lock()
        if self.dvc_dir:
            self.state_file = os.path.join(self.dvc_dir, self.STATE_FILE)
            self._lock_file = Lock(self.dvc_dir, self.STATE_LOCK_FILE)
        else:
            self.state_file = None
            self._lock_file = threading.Lock()
        self._db = self.load()

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
        if not self.state_file or not os.path.isfile(self.state_file):
            return {}

        with open(self.state_file, 'r') as fd:
            try:
                return json.load(fd)
            except ValueError as exc:
                Logger.error('Failed to load \'{}\''.format(self.state_file),
                             exc)
                return {}

    def dump(self):
        if not self.state_file:
            return

        with SignalHandler():
            with open(self.state_file, 'w+') as fd:
                json.dump(self._db, fd)

    @staticmethod
    def mtime(path):
        return str(int(nanotime.timestamp(os.path.getmtime(path))))

    @staticmethod
    def inode(path):
        return str(System.inode(path))

    def _do_update(self, path, dump=True):
        if not os.path.exists(path):
            return (None, None)

        mtime = self.mtime(path)
        inode = self.inode(path)

        md5 = self._get(inode, mtime)
        if md5:
            return (md5, None)

        md5, info = self._collect(path)
        state = StateEntry(md5, mtime)
        d = state.dumpd()

        with self._lock:
            with self._lock_file:
                self._db[inode] = d

                if dump:
                    self.dump()

        return (md5, info)

    def update(self, path, dump=True):
        return self._do_update(path, dump=dump)[0]

    def update_info(self, path, dump=True):
        md5, info = self._do_update(path, dump=dump)
        if not info:
            info = self.project.cache.local.load_dir_cache(md5)
        return (md5, info)

    def _get(self, inode, mtime):
        with self._lock:
            with self._lock_file:
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


class LinkStateEntry(object):
    PARAM_MTIME = 'mtime'
    PARAM_INODE = 'inode'

    def __init__(self, inode, mtime):
        self.mtime = mtime
        self.inode = inode

    @staticmethod
    def loadd(d):
        mtime = d[LinkStateEntry.PARAM_MTIME]
        inode = d[LinkStateEntry.PARAM_INODE]
        return LinkStateEntry(inode, mtime)

    def dumpd(self):
        return {
            self.PARAM_INODE: self.inode,
            self.PARAM_MTIME: self.mtime,
        }


class LinkState(State):
    STATE_FILE = 'link.state'
    STATE_LOCK_FILE = STATE_FILE + '.lock'

    def __init__(self, project):
        super(LinkState, self).__init__(project)
        self.root_dir = project.root_dir

    def update(self, path, dump=True):
        if not os.path.exists(path):
            return

        mtime = self.mtime(path)
        inode = self.inode(path)

        with self._lock:
            state = LinkStateEntry(inode, mtime)
            d = state.dumpd()
            self._db[os.path.relpath(path, self.root_dir)] = d
            if dump:
                with self._lock_file:
                    self.dump()

    def _do_remove_unused(self, used):
        items = self._db.copy().items()
        for p, s in items:
            path = os.path.join(self.root_dir, p)
            state = LinkStateEntry.loadd(s)

            if path in used:
                continue

            if not os.path.exists(path):
                continue

            inode = self.inode(path)
            mtime = self.mtime(path)

            if inode == state.inode and mtime == state.mtime:
                Logger.debug('Removing \'{}\' as unused link.'.format(path))
                remove(path)
                del self._db[p]

    def remove_unused(self, used):
        with self._lock:
            with self._lock_file:
                self._do_remove_unused(used)
                self.dump()
