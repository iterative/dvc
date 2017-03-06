from unittest import TestCase
import os
import shutil

from neatlynx.config import ConfigI
from neatlynx.data_file_obj import DataFileObj, NotInDataDirError, DataFilePathError
from neatlynx.git_wrapper import GitWrapperI


class BasicDataDirTest(TestCase):
    def setUp(self, test_dir, curr_dir=None):
        self._test_dir = os.path.realpath(test_dir)
        self._test_git_dir = os.path.join(self._test_dir, 'proj')
        self._old_curr_dir_abs = os.path.realpath(os.curdir)

        shutil.rmtree(self._test_dir, ignore_errors=True)

        if curr_dir:
            self._curr_dir = os.path.realpath(curr_dir)
            os.makedirs(self._curr_dir, exist_ok=True)
        else:
            self._curr_dir = self._test_git_dir

        os.makedirs(self._test_git_dir, exist_ok=True)
        os.chdir(self._curr_dir)

    def tearDown(self):
        os.chdir(self._old_curr_dir_abs)
        shutil.rmtree(self._test_dir, ignore_errors=True)


class TestDataFileObjBasic(BasicDataDirTest):
    def setUp(self):
        BasicDataDirTest.setUp(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI('data', 'cache', 'state')

        self._dobj = DataFileObj(os.path.join('data', 'file.txt'), git, config)
        pass

    def test_data_file(self):
        self.assertEqual(self._dobj.data_file_abs, os.path.join(self._test_git_dir, 'data', 'file.txt'))

    def test_cache_file(self):
        self.assertEqual(self._dobj.cache_file_abs, os.path.join(self._test_git_dir, 'cache', 'file.txt_ad45ba8'))

    def test_state_file(self):
        self.assertEqual(self._dobj.state_file_abs, os.path.join(self._test_git_dir, 'state', 'file.txt.state'))


class TestDataFileObjInDataDir(BasicDataDirTest):
    def setUp(self):
        BasicDataDirTest.setUp(self, os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='eeeff8f')
        config = ConfigI('da', 'ca', 'st')

        self._dobj = DataFileObj(os.path.join('da', 'dir1', 'd2', 'file.txt'), git, config)
        pass

    def test_data_file(self):
        target = os.path.join(self._test_git_dir, 'da', 'dir1', 'd2', 'file.txt')
        self.assertEqual(self._dobj.data_file_abs, target)

    def test_cache_file(self):
        target = os.path.join(self._test_git_dir, 'ca', 'dir1', 'd2', 'file.txt_eeeff8f')
        self.assertEqual(self._dobj.cache_file_abs, target)

    def test_state_file(self):
        target = os.path.join(self._test_git_dir, 'st', 'dir1', 'd2', 'file.txt.state')
        self.assertEqual(self._dobj.state_file_abs, target)


class TestDataFileObjLongPath(BasicDataDirTest):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp', 'ntx_unit_test', 'proj', 'mydir', 'dd3')

        BasicDataDirTest.setUp(self,
                               os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                               curr_dir)

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        self._config = ConfigI('data', 'cache', 'state')

        self._dobj = DataFileObj(os.path.join('..', '..', 'data', 'file1.txt'),
                                 self._git, self._config)
        pass

    def test_data_file(self):
        target = os.path.join(self._test_git_dir, 'data', 'file1.txt')
        self.assertEqual(self._dobj.data_file_abs, target)

    def test_cache_file(self):
        target = os.path.join(self._test_git_dir, 'cache', 'file1.txt_123ed8')
        self.assertEqual(self._dobj.cache_file_abs, target)

    def test_state_file(self):
        target = os.path.join(self._test_git_dir, 'state', 'file1.txt.state')
        self.assertEqual(self._dobj.state_file_abs, target)

    def test_file_name_only(self):
        with self.assertRaises(NotInDataDirError):
            DataFileObj('file.txt', self._git, self._config)
        pass

    def test_relative_git_dir(self):
        with self.assertRaises(NotInDataDirError):
            DataFileObj('data/file.txt', self._git, self._config)
        pass

    def test_relative_path_error(self):
        with self.assertRaises(NotInDataDirError):
            DataFileObj('../data/file.txt', self._git, self._config)
        pass


class RunOutsideGitRepoTest(BasicDataDirTest):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp')

        BasicDataDirTest.setUp(self,
                               os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                               curr_dir)

    def test_outside_git_dir(self):
        git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        config = ConfigI('data', 'cache', 'state')

        with self.assertRaises(NotInDataDirError):
            DataFileObj('file.txt', git, config)
        pass
