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
        self.file1_code_file = 'file1.py'
        self.create_file_and_commit(self.file1_code_file, 'An awesome code...')
        cmd_file1 = CmdRun(args=[
                                'printf',
                                'Hello\nMary',
                                '--not-repro',
                                '--stdout',
                                self.file_name1,
                                '--code',
                                self.file1_code_file
                            ],
                            parse_config=False,
                            config_obj=self.config,
                            git_obj=self.git)
        self.assertEqual(cmd_file1.code_dependencies, [self.file1_code_file])
        cmd_file1.run()

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

        self.file_res_code_file = 'code_res.py'
        self.create_file_and_commit(self.file_res_code_file, 'Another piece of code')
        self.file_name_res = 'data/file_res'
        cmd_res = CmdRun(args=[
                            'cat',
                            self.file_name11,
                            self.file_name2,
                            '--stdout',
                            self.file_name_res,
                            '--code',
                            self.file_res_code_file
                         ],
                         parse_config=False,
                         config_obj=self.config,
                         git_obj=self.git)
        self.assertEqual(cmd_res.code_dependencies, [self.file_res_code_file])
        cmd_res.run()

        self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')

    def create_file_and_commit(self, file_name, content='Any', message='Just a commit'):
        self.create_file(file_name, content)
        self.commit_file(file_name, message)

    @staticmethod
    def commit_file(file_name, message='Just a commit'):
        Executor.exec_cmd_only_success(['git', 'add', file_name])
        Executor.exec_cmd_only_success(['git', 'commit', '-m', message])

    def modify_file_and_commit(self, filename, content_to_add=' '):
        fd = open(filename, 'a')
        fd.write(content_to_add)
        fd.close()
        self.commit_file(filename)


class ReproCodeDependencyTest(ReproBasicEnv):

    def test(self):
        self.modify_file_and_commit(self.file_res_code_file)

        CmdRepro(args=[self.file_name_res],
                 config_obj=self.config,
                 git_obj=self.git
        ).run()

        self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')


# class ReproChangedDependency(ReproBasicEnv):
#     def test(self):
#
#         self.assertEqual(open(self.file_name_res).read(), 'Hello\nBobby')
#
#         CmdDataRemove(args=[self.file_name1, '--keep-in-cloud'],
#                       config_obj=self.config,
#                       git_obj=self.git
#         ).run()
#
#         CmdRun(args=['printf', 'Goodbye\nWorld!', '--stdout', self.file_name1],
#                config_obj=self.config,
#                git_obj=self.git
#         ).run()
#
#         CmdRepro(args=[self.file_name_res],
#                  config_obj=self.config,
#                  git_obj=self.git
#         ).run()
#
#         # self.assertEqual(open(self.file_name_res).read(), 'Goodbye\nBobby')
