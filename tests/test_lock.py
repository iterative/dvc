from dvc.lock import Lock

from tests.basic_env import TestDvc


class TestLock(TestDvc):
    def test_lock(self):
        lock = Lock(self.dvc.dvc_dir)
        lock.lock()
        self.assertTrue(lock._lock.exists())
        self.assertTrue(lock._lock.acquired)
        lock.unlock()
        self.assertFalse(lock._lock.acquired)

    def test_with(self):
        lock = Lock(self.dvc.dvc_dir)
        with lock:
            self.assertTrue(lock._lock.acquired)
        self.assertFalse(lock._lock.acquired)
