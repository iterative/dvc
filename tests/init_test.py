import os

from dvc.main import main
from dvc.command.init import CmdInit
from dvc.project import Project

from tests.basic_env import TestGit


class TestInit(TestGit):
    def _test_init(self):
        self.assertTrue(os.path.exists(Project.DVC_DIR))
        self.assertTrue(os.path.isdir(Project.DVC_DIR))

    def test_api(self):
        Project.init()
        
        self._test_init()

    def test_cli(self):
        ret = main(['init'])
        self.assertEqual(ret, 0)

        self._test_init()
