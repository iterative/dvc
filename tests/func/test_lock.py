from dvc.lock import LockError
from dvc.main import main
from dvc.lock import Lock

from tests.basic_env import TestDvc


class TestLock(TestDvc):
    def test_with(self):
        lock = Lock(self.dvc.dvc_dir)
        with lock:
            with self.assertRaises(LockError):
                lock2 = Lock(self.dvc.dvc_dir)
                with lock2:
                    self.assertTrue(False)

    def test_cli(self):
        lock = Lock(self.dvc.dvc_dir)
        with lock:
            ret = main(["add", self.FOO])
            self.assertEqual(ret, 1)
