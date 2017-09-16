import os

from dvc.command.target import CmdTarget
from dvc.config import ConfigI
from dvc.git_wrapper import GitWrapperI
from dvc.path.factory import PathFactory
from dvc.settings import Settings
from dvc.system import System
from tests.basic_env import BasicEnvironment


class TestCmdTarget(BasicEnvironment):
    def setUp(self):
        self._test_dir = os.path.join(os.path.sep, 'tmp', 'dvc_unit_test')
        curr_dir = None
        commit = 'abc1234'

        BasicEnvironment.init_environment(self, self._test_dir, curr_dir)

        self._commit = commit

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit=self._commit)
        self._config = ConfigI('data')
        self.path_factory = PathFactory(self._git, self._config)
        self.settings = Settings(['target'], self._git, self._config)

        os.chdir(self._test_git_dir)

        os.mkdir(os.path.join('data', 'dir1'))
        os.mkdir(os.path.join('.dvc', 'cache', 'dir1'))

        self.file1_cache = os.path.join('.dvc', 'cache', 'dir1', 'file1')
        self.file1_data = os.path.join('data', 'dir1', 'file1')

        open(self.file1_cache, 'w').write('ddfdff')
        System.symlink(self.file1_cache, self.file1_data)

        self.file2_cache = os.path.join('.dvc', 'cache', 'file2')
        self.file2_data = os.path.join('data', 'file2')

        open(self.file2_cache, 'w').write('ddfdff')
        System.symlink(self.file2_cache, self.file2_data)

    def test_initial_default_target(self):
        self.assertFalse(os.path.exists('.dvc/target'))

    def test_single_file(self):
        self.settings.parse_args('target {}'.format(self.file1_data))
        cmd = CmdTarget(self.settings)

        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), self.file1_data)

    def test_multiple_files(self):
        self.settings.parse_args('target {}'.format(self.file1_data))
        cmd = CmdTarget(self.settings)
        cmd.run()

        # Another target
        self.settings.parse_args('target {}'.format(self.file2_data))
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), self.file2_data)

        # Unset target
        self.settings.parse_args('target --unset')
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), '')

    def test_initial_unset(self):
        self.settings.parse_args('target --unset')
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 1)
        self.assertFalse(os.path.exists(os.path.join('.dvc', '.target')))

    def test_unset_existing_target(self):
        self.settings.parse_args('target {}'.format(self.file1_data))
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), self.file1_data)

        self.settings.parse_args('target --unset')
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), '')

    def test_the_same(self):
        self.settings.parse_args('target {}'.format(self.file1_data))
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), self.file1_data)

        self.assertEqual(cmd.run(), 0)
        self.assertEqual(open(os.path.join('.dvc', 'target')).read(), self.file1_data)

    def test_args_conflict(self):
        self.settings.parse_args('target {} --unset'.format(self.file1_data))
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 1)

    def test_no_args(self):
        self.settings.parse_args('target --unset')
        cmd = CmdTarget(self.settings)
        self.assertEqual(cmd.run(), 1)
