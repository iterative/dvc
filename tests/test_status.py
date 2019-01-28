import os

from dvc.main import main

from tests.basic_env import TestDvc


class TestStatus(TestDvc):
    def test_quiet(self):
        self.dvc.add(self.FOO)

        ret = main(['status', '--quiet'])
        self.assertEqual(ret, 0)

        os.remove(self.FOO)
        os.rename(self.BAR, self.FOO)

        ret = main(['status', '--quiet'])
        self.assertEqual(ret, 1)
