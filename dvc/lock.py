import os
import fasteners
from dvc.exceptions import DvcException


class LockError(DvcException):
    pass


class Lock(object):
    LOCK_FILE = 'lock'
    TIMEOUT = 5

    def __init__(self, dvc_dir):
        self.lock_file = os.path.join(dvc_dir, self.LOCK_FILE)
        self._lock = fasteners.InterProcessLock(self.lock_file)

    @staticmethod
    def init(dvc_dir):
        return Lock(dvc_dir)

    def lock(self):
        if not self._lock.acquire(timeout=self.TIMEOUT):
            raise LockError('Cannot perform the cmd since DVC is busy and locked. Please retry the cmd later.')

    def unlock(self):
        self._lock.release()

    def __enter__(self):
        self.lock()

    def __exit__(self, type, value, tb):
        self.unlock()
