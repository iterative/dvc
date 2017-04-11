import os
import shutil
from unittest import TestCase

from dvc.path.path import Path
from dvc.git_wrapper import GitWrapperI
from dvc.system import System


class TestDvcPathTest(TestCase):
    def setUp(self):
        self.curr_dir = System.realpath('.')
        self.test_dir = System.realpath('/tmp/ntx_unit_test/dvc_path')
        self.tearDown()
        os.makedirs(os.path.join(self.test_dir, 'data'))
        os.makedirs(os.path.join(self.test_dir, 'code', 'lib'))
        os.makedirs(os.path.join(self.test_dir, 'd1', 'd2', 'dir3', 'd4', 'dir5'))

        self._git = GitWrapperI(self.test_dir)

    def tearDown(self):
        os.chdir(self.curr_dir)
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _validate_dvc_path(self, path, dvc_file_name, relative_file_name):
        self.assertEqual(path.dvc, dvc_file_name)
        self.assertEqual(path.filename, os.path.basename(dvc_file_name))
        self.assertEqual(path.relative, relative_file_name)
        self.assertTrue(os.path.isabs(path.abs[0]))
        self.assertTrue(path.abs.endswith(dvc_file_name))

    def basic_test(self):
        os.chdir(self.test_dir)

        file = os.path.join('data', 'file.txt')
        path = Path(file, self._git)
        self._validate_dvc_path(path, file, file)
        pass

    def from_dir_test(self):
        os.chdir(os.path.join(self.test_dir, 'code'))

        file_dvc_path = os.path.join('data', 'file1.txt')
        file_relative_path = os.path.join('..', file_dvc_path)

        path = Path(file_relative_path, self._git)
        self._validate_dvc_path(path, file_dvc_path, file_relative_path)
        pass

    def from_deep_dirs_test(self):
        deep_dir = os.path.join('d1', 'd2', 'dir3')
        os.chdir(os.path.join(self.test_dir, deep_dir))

        file_dvc = os.path.join('code', 'lib', 'context_switcher_structs.asm')
        file_relative = os.path.join('..', '..', '..', file_dvc)

        path = Path(file_relative, self._git)
        self._validate_dvc_path(path, file_dvc, file_relative)
        pass

    def go_deeper_test(self):
        deep_dir = os.path.join('d1', 'd2', 'dir3')
        os.chdir(os.path.join(self.test_dir, deep_dir))

        file_relative_path = os.path.join(deep_dir, 'd4', 'dir5', 'rawdata.tsv')
        file_dvc_path = os.path.join(deep_dir, file_relative_path)

        path = Path(file_relative_path, self._git)
        self._validate_dvc_path(path, file_dvc_path, file_relative_path)
        pass
