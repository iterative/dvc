import os

from dvc.config import ConfigI
from dvc.git_wrapper import GitWrapperI
from dvc.path.data_item import NotInDataDirError, DataItemError, DataItem
from dvc.path.factory import PathFactory
from dvc.system import System
from tests.basic_env import BasicEnvironment


class TestDataFileObjBasic(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI('data', 'cache', 'state')

        file = os.path.join('data', 'file.txt')
        self._data_path = DataItem(file, git, config)
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


class TestPathFactory(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(git, config)

        self.data_file = os.path.join('data', 'file.txt')
        self.cache_file = os.path.join('cache', 'fsymlinc.txt')

        fd = open(self.cache_file, 'w+')
        fd.write('some text')
        fd.close()

        os.chdir('data')
        System.symlink(os.path.join('..', self.cache_file), 'file.txt')
        os.chdir('..')
        pass

    def test_data_file_factory(self):
        data_path = self.path_factory.data_item(self.data_file)
        self.assertEqual(data_path.data.dvc, self.data_file)

        indirect_file_path = os.path.join('..', self._proj_dir, self.data_file)
        data_path_indirect = self.path_factory.data_item(indirect_file_path)
        self.assertEqual(data_path_indirect.data.dvc, self.data_file)
        pass

    def test_data_symlink_factory(self):
        data_path = self.path_factory.existing_data_item(self.data_file)
        self.assertEqual(data_path.cache.dvc, self.cache_file)
        pass

    def test_data_symlink_factory_cache(self):
        data_path = self.path_factory.existing_data_item(self.data_file)
        self.assertEqual(data_path.data.dvc, self.data_file)
        pass

    def test_data_symlink_factory_exception(self):
        with self.assertRaises(DataItemError):
            self.path_factory.existing_data_item(self.cache_file)
        pass

    def test_data_path(self):
        file = os.path.join('data', 'file1')
        path = self.path_factory.path(file)
        self.assertEqual(path.dvc, file)
        self.assertEqual(path.relative, file)
        self.assertTrue(path.abs.endswith(file))

    def test_to_data_path(self):
        exclude_file = os.path.join('cache', 'file2')
        data_path_file1 = os.path.join('data', 'file1')
        data_path_file2 = os.path.join('data', 'file2')
        files = [
            data_path_file1,
            exclude_file,
            data_path_file2
        ]

        data_path_list, exclude_file_list = self.path_factory.to_data_items(files)

        self.assertEqual(len(data_path_list), 2)
        self.assertEqual(len(exclude_file_list), 1)

        self.assertEqual(exclude_file_list[0], exclude_file)
        data_path_set = set(x.data.dvc for x in data_path_list)
        self.assertEqual(data_path_set, {data_path_file1, data_path_file2})


class TestDataPathInDataDir(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='eeeff8f')
        self.data_dir = 'da'
        config = ConfigI(self.data_dir, 'ca', 'st')
        self.path_factory = PathFactory(git, config)

        deep_path = os.path.join(self.data_dir, 'dir1', 'd2', 'file.txt')
        self.data_path = self.path_factory.data_item(deep_path)
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
        expected = os.path.join('..', '..', '..', 'ca', 'dir1', 'd2', 'file.txt_eeeff8f')
        self.assertEqual(self.data_path.symlink_file, expected)

    def test_data_dir(self):
        data_path = self.path_factory.data_item(self.data_dir)
        self.assertEqual(data_path.data.dvc, self.data_dir)
        self.assertEqual(data_path.data_dvc_short, '')


class TestDataFileObjLongPath(BasicEnvironment):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp', 'ntx_unit_test', 'proj', 'mydir', 'dd3')

        BasicEnvironment.init_environment(self,
                                          os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                                          curr_dir)

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        self._config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(self._git, self._config)

        self.data_path = self.path_factory.data_item(os.path.join('..', '..', 'data', 'file1.txt'))
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
            self.path_factory.data_item('file.txt')
        pass

    def test_relative_git_dir(self):
        with self.assertRaises(NotInDataDirError):
            self.path_factory.data_item('data/file.txt')
        pass

    def test_relative_path_error(self):
        with self.assertRaises(NotInDataDirError):
            self.path_factory.data_item('../data/file.txt')
        pass


class RunOutsideGitRepoTest(BasicEnvironment):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp')

        BasicEnvironment.init_environment(self,
                                          os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                                          curr_dir)

    def test_outside_git_dir(self):
        git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        config = ConfigI('data', 'cache', 'state')
        path_factory = PathFactory(git, config)

        with self.assertRaises(NotInDataDirError):
            path_factory.data_item('file.txt')
        pass
