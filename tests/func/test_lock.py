import os

from dvc.lock import Lock
from dvc.lock import LockError
from dvc.main import main
from tests.basic_env import TestDvc


class TestLock(TestDvc):
    def test_with(self):
        lockfile = os.path.join(self.dvc.dvc_dir, "lock")
        lock = Lock(lockfile)
        with lock:
            with self.assertRaises(LockError):
                lock2 = Lock(lockfile)
                with lock2:
                    self.assertTrue(False)

    def test_cli(self):
        lockfile = os.path.join(self.dvc.dvc_dir, "lock")
        lock = Lock(lockfile)
        with lock:
            ret = main(["add", self.FOO])
            self.assertEqual(ret, 1)
