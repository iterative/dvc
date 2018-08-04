import os
import time
import zc.lockfile

from dvc.exceptions import DvcException


class LockError(DvcException):
    pass


class Lock(object):
    LOCK_FILE = 'lock'
    TIMEOUT = 5

    def __init__(self, dvc_dir, name=LOCK_FILE):
        self.lock_file = os.path.join(dvc_dir, name)
        self._lock = None

    def _do_lock(self):
        try:
            self._lock = zc.lockfile.LockFile(self.lock_file)
        except zc.lockfile.LockError:
            raise LockError('Cannot perform the cmd since DVC is busy and '
                            'locked. Please retry the cmd later.')

    def lock(self):
        try:
            self._do_lock()
            return
        except LockError:
            time.sleep(self.TIMEOUT)

        self._do_lock()

    def unlock(self):
        self._lock.close()
        self._lock = None

    def __enter__(self):
        self.lock()

    def __exit__(self, type, value, tb):
        self.unlock()
