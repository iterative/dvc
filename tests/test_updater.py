import os

from tests.basic_env import TestDvc


class TestUpdater(TestDvc):
    def test(self):
        # NOTE: only test on travis CRON to avoid generating too much logs
        travis = os.getenv('TRAVIS') == 'true'
        if not travis:
            return

        cron = os.getenv('TRAVIS_EVENT_TYPE') == 'cron'
        if not cron:
            return

        env = os.environ.copy()
        if os.getenv('CI'):
            del os.environ['CI']

        self.dvc.updater.check()
        self.dvc.updater.check()
        self.dvc.updater.check()

        os.environ = env.copy()
