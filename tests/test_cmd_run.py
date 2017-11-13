import os
import tempfile
from unittest import TestCase

from dvc.command.run import CmdRun, RunError
from dvc.config import ConfigI
from dvc.executor import Executor
from dvc.path.data_item import DataItem
from dvc.path.factory import PathFactory
from dvc.git_wrapper import GitWrapper
from dvc.settings import Settings
from dvc.system import System
from dvc.utils import rmtree


class RunBasicTest(TestCase):
    def setUp(self):
        self.test_dir = System.get_long_path(tempfile.mkdtemp())
        self._old_curr_dir_abs = System.realpath(os.curdir)

        self.tearDown()
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        os.mkdir('data')
        os.mkdir(ConfigI.CONFIG_DIR)
        os.mkdir(os.path.join(ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR_NAME))
        os.mkdir(os.path.join(ConfigI.CONFIG_DIR, ConfigI.STATE_DIR_NAME))

        self.init_git_repo()
        self.git = GitWrapper()

        self.config = ConfigI('data')
        self.path_factory = PathFactory(self.git, self.config)

        self.settings = Settings('run cmd', self.git, self.config)
        pass

    def init_git_repo(self):
        Executor.exec_cmd_only_success(['git', 'init'])
        msg = os.path.join(ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR_NAME)
        msg += '\n'
        msg += os.path.join(ConfigI.CONFIG_DIR, '.' + ConfigI.CONFIG + '.lock')
        self.create_file('.gitignore', msg)
        Executor.exec_cmd_only_success(['git', 'add', '.gitignore'])
        Executor.exec_cmd_only_success(['git', 'commit', '-m', '"Init test repo"'])

    @staticmethod
    def create_file(file2, content='random text'):
        fd = open(file2, 'w+')
        fd.write(content)
        fd.close()

    def tearDown(self):
        rmtree(self.test_dir)
        os.chdir(self._old_curr_dir_abs)


class RunTwoFilesBase(RunBasicTest):
    def setUp(self):
        super(RunTwoFilesBase, self).setUp()

        self.input_param_file = os.path.join('data', 'extra_input')
        self.extra_output_file = os.path.join('data', 'extra_output')

        self.settings.parse_args('run --input {} --output {} cmd'.format(self.input_param_file, self.extra_output_file))
        self.settings._args = []
        cmd_run = CmdRun(self.settings)

        self.file_name1 = os.path.join('data', 'file1')
        self.file_name2 = os.path.join('data', 'file2')
        self.state_objs = cmd_run.run_command(['echo', 'test'], [], [], shell=True,
                                              stdout=self.file_name1,
                                              stderr=self.file_name2)

        self.state_file_name1 = os.path.join(ConfigI.CONFIG_DIR, ConfigI.STATE_DIR_NAME, self.file_name1 + DataItem.STATE_FILE_SUFFIX)
        self.state_file_name2 = os.path.join(ConfigI.CONFIG_DIR, ConfigI.STATE_DIR_NAME, self.file_name2 + DataItem.STATE_FILE_SUFFIX)

        self.state_file1 = None
        self.state_file2 = None
        for s in self.state_objs:
            if s.file == self.state_file_name1:
                self.state_file1 = s
            elif s.file == self.state_file_name2:
                self.state_file2 = s
        pass


class TestRunStateFiles(RunTwoFilesBase):
    def test(self):
        names = [x.file for x in self.state_objs]
        self.assertEqual({self.state_file_name1, self.state_file_name2}, set(names))


class TestRunExtraInExtraOutFiles(RunTwoFilesBase):
    def test(self):
        self.assertIsNotNone(self.state_file1)
        self.assertEqual(self.state_file1.input_files, [self.input_param_file])

        output_set = {self.file_name1, self.file_name2, self.extra_output_file}
        self.assertEqual(set(self.state_file1.output_files), output_set)
