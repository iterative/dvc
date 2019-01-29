import os

from dvc.system import System
from tests.basic_env import TestDir


class TestSystem(TestDir):
    def test_getdirinfo(self):
        if os.name != "nt":
            return

        # simulate someone holding an exclussive access
        locked = "locked"
        fd = os.open(locked, os.O_WRONLY | os.O_CREAT | os.O_EXCL)

        # should not raise WinError
        System.getdirinfo(locked)

        os.close(fd)
