import os

from dvc.updater import Updater

from tests.basic_env import TestDvc


class TestDvcUpdater(TestDvc):
    def test(self):
        # NOTE: need to temporarily unset CI env to allow updater to work
        env = os.environ.copy()
        os.environ['CI'] = 'False'

        self.dvc.updater.check()
        self.dvc.updater.check()
        self.dvc.updater.check()

        os.environ = env
