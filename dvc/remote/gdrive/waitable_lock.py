from time import time, sleep
from threading import Lock

import zc.lockfile


class WaitableLock(object):
    def __init__(self, lock_file, timeout=5):
        self.lock_file = lock_file
        self.timeout = timeout
        self._thread_lock = Lock()
        self._lock = None

    def __enter__(self):
        t0 = time()
        self._thread_lock.acquire()
        while time() - t0 < self.timeout:
            try:
                self._lock = zc.lockfile.LockFile(self.lock_file)
            except zc.lockfile.LockError:
                sleep(0.5)
            else:
                return
        self._lock = zc.lockfile.LockFile(self.lock_file)

    def __exit__(self, typ, value, tbck):
        self._lock.close()
        self._lock = None
        self._thread_lock.release()
