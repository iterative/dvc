import os
import fasteners

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
        self._lock.acquire(timeout=self.TIMEOUT)

    def unlock(self):
        self._lock.release()

    def __enter__(self):
        self.lock()

    def __exit__(self, type, value, tb):
        self.unlock()
