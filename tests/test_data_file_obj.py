from unittest import TestCase
import os
import shutil

from dvc.config import ConfigI
from dvc.path.data_file_obj import NotInDataDirError, DataFilePathError, DataPath
from dvc.git_wrapper import GitWrapperI
from dvc.path.path_factory import PathFactory


class BasicDataDirTest(TestCase):
    def setUp(self, test_dir, curr_dir=None):
        self._test_dir = os.path.realpath(test_dir)
        self._proj_dir = 'proj'
        self._test_git_dir = os.path.join(self._test_dir, self._proj_dir)
        self._old_curr_dir_abs = os.path.realpath(os.curdir)

        shutil.rmtree(self._test_dir, ignore_errors=True)

        if curr_dir:
            self._curr_dir = os.path.realpath(curr_dir)
        else:
            self._curr_dir = self._test_git_dir

        if not os.path.exists(self._curr_dir):
            os.makedirs(self._curr_dir)

        if not os.path.isdir(self._test_git_dir):
            os.makedirs(self._test_git_dir)

        data_dir = os.path.join(self._test_git_dir, 'data')
        if not os.path.isdir(data_dir):
            os.makedirs(data_dir)

        os.chdir(self._curr_dir)

    def tearDown(self):
        os.chdir(self._old_curr_dir_abs)
        shutil.rmtree(self._test_dir, ignore_errors=True)


class TestDataFileObjBasic(BasicDataDirTest):
    def setUp(self):
        BasicDataDirTest.setUp(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI('data', 'cache', 'state')

        file = os.path.join('data', 'file.txt')
        self._data_path = DataPath(file, git, config)
        pass

    def test_data_file(self):
        data_file_full_path = os.path.join(self._test_git_dir, 'data', 'file.txt')
        self.assertEqual(self._data_path.data.abs, data_file_full_path)

    def test_cache_file(self):
        cache_file_full_path = os.path.join(self._test_git_dir, 'cache', 'file.txt_ad45ba8')
        self.assertEqual(self._data_path.cache.abs, cache_file_full_path)

    def test_state_file(self):
        state_file_full_path = os.path.join(self._test_git_dir, 'state', 'file.txt.state')
        self.assertEqual(self._data_path.state.abs, state_file_full_path)

    def test_data_dvs_short(self):
        self.assertEqual(self._data_path.data_dvc_short, 'file.txt')
        pass


class TestPathFactory(BasicDataDirTest):
    def setUp(self):
        BasicDataDirTest.setUp(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(git, config)

        self.file = os.path.join('data', 'file.txt')

        fd = open(self.file, 'w+')
        fd.write('some text')
        fd.close()

        self.file_symlink = os.path.join('data', 'fsymlinc.txt')
        os.chdir('data')
        os.symlink('file.txt', 'fsymlinc.txt')
        os.chdir('..')
        pass

    def test_data_file_factory(self):
        data_path = self.path_factory.data_path(self.file)
        self.assertEqual(data_path.data.dvc, self.file)

        indirect_file_path = os.path.join('..', self._proj_dir, self.file)
        data_path_indirect = self.path_factory.data_path(indirect_file_path)
        self.assertEqual(data_path_indirect.data.dvc, self.file)
        pass

    def test_data_symlink_factory(self):
        data_path = self.path_factory.existing_data_path(self.file_symlink)
        self.assertEqual(data_path.data.dvc, self.file_symlink)
        pass

    def test_data_symlink_factory_cache(self):
        data_path = self.path_factory.existing_data_path(self.file_symlink)
        self.assertEqual(data_path.cache.dvc, self.file)
        pass

    def test_data_symlink_factory_exception(self):
        with self.assertRaises(DataFilePathError):
            self.path_factory.existing_data_path(self.file)
        pass


class TestDataPathInDataDir(BasicDataDirTest):
    def setUp(self):
        BasicDataDirTest.setUp(self, os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='eeeff8f')
        config = ConfigI('da', 'ca', 'st')
        path_factory = PathFactory(git, config)

        deep_path = os.path.join('da', 'dir1', 'd2', 'file.txt')
        self.data_path = path_factory.data_path(deep_path)
        pass

    def test_data_file(self):
        target = os.path.join(self._test_git_dir, 'da', 'dir1', 'd2', 'file.txt')
        self.assertEqual(self.data_path.data.abs, target)

    def test_cache_file(self):
        target = os.path.join(self._test_git_dir, 'ca', 'dir1', 'd2', 'file.txt_eeeff8f')
        self.assertEqual(self.data_path.cache.abs, target)

    def test_state_file(self):
        target = os.path.join(self._test_git_dir, 'st', 'dir1', 'd2', 'file.txt.state')
        self.assertEqual(self.data_path.state.abs, target)

    def test_symlink(self):
        self.assertEqual(self.data_path._symlink_file, '../../../ca/dir1/d2/file.txt_eeeff8f')


class TestDataFileObjLongPath(BasicDataDirTest):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp', 'ntx_unit_test', 'proj', 'mydir', 'dd3')

        BasicDataDirTest.setUp(self,
                               os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                               curr_dir)

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        self._config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(self._git, self._config)

        # self._dobj = DataFileObj(os.path.join('..', '..', 'data', 'file1.txt'),
        #                          self._git, self._config)
        self.data_path = self.path_factory.data_path(os.path.join('..', '..', 'data', 'file1.txt'))
        pass

    def test_data_file(self):
        target = os.path.join(self._test_git_dir, 'data', 'file1.txt')
        self.assertEqual(self.data_path.data.abs, target)

    def test_cache_file(self):
        target = os.path.join(self._test_git_dir, 'cache', 'file1.txt_123ed8')
        self.assertEqual(self.data_path.cache.abs, target)

    def test_state_file(self):
        target = os.path.join(self._test_git_dir, 'state', 'file1.txt.state')
        self.assertEqual(self.data_path.state.abs, target)

    def test_file_name_only(self):
        with self.assertRaises(NotInDataDirError):
            self.path_factory.data_path('file.txt')
        pass

    def test_relative_git_dir(self):
        with self.assertRaises(NotInDataDirError):
            self.path_factory.data_path('data/file.txt')
        pass

    def test_relative_path_error(self):
        with self.assertRaises(NotInDataDirError):
            self.path_factory.data_path('../data/file.txt')
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
        path_factory = PathFactory(git, config)

        with self.assertRaises(NotInDataDirError):
            path_factory.data_path('file.txt')
        pass
