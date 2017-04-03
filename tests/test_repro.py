import os
import shutil
import subprocess
import tempfile
from unittest import TestCase

import sys

from dvc.command.remove import CmdDataRemove
from dvc.command.repro import ReproChange, CmdRepro
from dvc.command.run import CmdRun, RunError
from dvc.config import ConfigI
from dvc.executor import Executor
from dvc.path.data_item import DataItem
from dvc.path.factory import PathFactory
from dvc.git_wrapper import GitWrapperI, GitWrapper
from dvc.repository_change import RepositoryChange
from dvc.state_file import StateFile
from tests.test_cmd_run import RunBasicTest


# class ReproBasicEnv(TestCase):
#     def setUp(self):
#         self.test_dir = tempfile.mkdtemp()
#         self._old_curr_dir_abs = os.path.realpath(os.curdir)
#
#         self.tearDown()
#         os.mkdir(self.test_dir)
#         os.chdir(self.test_dir)
#         os.mkdir('data')
#         os.mkdir('cache')
#         os.mkdir('state')
#
#         self.init_git_repo()
#         self.git = GitWrapper()
#
#         self.config = ConfigI('data', 'cache', 'state')
#         self.path_factory = PathFactory(self.git, self.config)
#         pass
#
#     def init_git_repo(self):
#         Executor.exec_cmd_only_success(['git', 'init'])
#         self.create_file('.gitignore', 'cache\n.dvc.conf.lock')
#         Executor.exec_cmd_only_success(['git', 'add', '.gitignore'])
#         Executor.exec_cmd_only_success(['git', 'commit', '-m', '"Init test repo"'])
#
#     def tearDown(self):
#         shutil.rmtree(self.test_dir, ignore_errors=True)
#         os.chdir(self._old_curr_dir_abs)
#
#     @staticmethod
#     def create_file(file2, content='random text'):
#         fd = open(file2, 'w+')
#         fd.write(content)
#         fd.close()


# class RunEndToEndTest()

class ReproBasicEnv(RunBasicTest):
    def setUp(self):
        super(ReproBasicEnv, self).setUp()
        self.file_name1 = 'data/file1'
        CmdRun(args=['printf', 'Hello\nWorld!', '--not-repro', '--stdout', self.file_name1],
               parse_config=False,
               config_obj=self.config,
               git_obj=self.git
        ).run()

        self.file_name11 = 'data/file11'
        CmdRun(args=['head', '-n', '1', self.file_name1, '--stdout', self.file_name11],
               parse_config=False,
               config_obj=self.config,
               git_obj=self.git
        ).run()

        self.file_name2 = 'data/file2'
        CmdRun(args=['printf', 'Bobby', '--not-repro', '--stdout', self.file_name2],
               parse_config=False,
               config_obj=self.config,
               git_obj=self.git
        ).run()

        self.file_name_res = 'data/file_res'
        CmdRun(args=['cat', self.file_name11, self.file_name2, '--stdout', self.file_name_res],
               parse_config=False,
               config_obj=self.config,
               git_obj=self.git
        ).run()

        self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')

#
# class ReproRemovedFileTest(ReproBasicEnv):
#     def test(self):
#         # CmdDataRemove(args=[self.file_name_res, '--keep-in-cloud'],
#         #               config_obj=self.config,
#         #               git_obj=self.git
#         # ).run()
#
#         # CmdRun(args=['printf', 'Goodbye\nWorld!', '--stdout', self.file_name1],
#         #        config_obj=self.config,
#         #        git_obj=self.git
#         # ).run()
#
#         CmdRepro(args=[self.file_name_res],
#                  config_obj=self.config,
#                  git_obj=self.git
#         ).run()
#
#         self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')


class ReproChangedDependency(ReproBasicEnv):
    def test(self):

        self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')

        CmdDataRemove(args=[self.file_name1, '--keep-in-cloud'],
                      config_obj=self.config,
                      git_obj=self.git
        ).run()

        CmdRun(args=['printf', 'Goodbye\nWorld!', '--stdout', self.file_name1],
               config_obj=self.config,
               git_obj=self.git
        ).run()

        CmdRepro(args=[self.file_name_res],
                 config_obj=self.config,
                 git_obj=self.git
        ).run()

        # self.assertEqual(open(self.file_name_res).read(), 'Goodbye\nBobby')
