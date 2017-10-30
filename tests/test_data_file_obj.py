import os

from dvc.config import ConfigI
from dvc.git_wrapper import GitWrapperI
from dvc.path.data_item import DataDirError, DataItemError, DataItem
from dvc.path.factory import PathFactory
from dvc.system import System
from tests.basic_env import BasicEnvironment


class TestDataFileObjBasic(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI()

        file = os.path.join('data', 'file.txt')
        self.cache_file_full_path = os.path.join(self._test_git_dir, ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR, 'file.txt_ad45ba8')
        self._data_path = DataItem(file, git, config, cache_file=self.cache_file_full_path)
        pass

    def test_data_file(self):
        data_file_full_path = os.path.join(self._test_git_dir, 'data', 'file.txt')
        self.assertEqual(self._data_path.data.abs, data_file_full_path)

    def test_cache_file(self):
        self.assertEqual(self._data_path.cache.abs, self.cache_file_full_path)

    def test_state_file(self):
        state_file_full_path = os.path.join(self._test_git_dir, ConfigI.CONFIG_DIR, ConfigI.STATE_DIR, 'data', 'file.txt.state')
        self.assertEqual(self._data_path.state.abs, state_file_full_path)


class TestDataItemWithGivenCache(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        self._config = ConfigI('data')

        self._file = os.path.join('data', 'file.txt')
        self._cache = os.path.join(ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR, 'file.txt_abcd')

        self._data_item = DataItem(self._file, self._git, self._config, self._cache)
        pass

    def test_basic(self):
        self.assertEqual(self._data_item.data.relative, self._file)
        self.assertEqual(self._data_item.cache.relative, self._cache)


class TestDeepDataItemWithGivenCache(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        self._config = ConfigI('data')

        self._file = os.path.join('data', 'dir1', 'file.txt')
        self._cache = os.path.join(ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR, 'dir1', 'file.txt_abcd')

        self._data_item = DataItem(self._file, self._git, self._config, self._cache)
        pass

    def test_basic(self):
        self.assertEqual(self._data_item.data.relative, self._file)
        self.assertEqual(self._data_item.cache.relative, self._cache)


class TestPathFactory(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='ad45ba8')
        config = ConfigI()
        self.path_factory = PathFactory(git, config)

        self.data_file = os.path.join('data', 'file.txt')
        dummy_md5 = 'fsymlinc.txt'
        self.cache_file = os.path.join(ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR, dummy_md5)
        self.state_file = os.path.join(ConfigI.CONFIG_DIR, ConfigI.STATE_DIR, 'data', 'file.txt' + DataItem.STATE_FILE_SUFFIX)

        with open(self.cache_file, 'w+') as fd:
            fd.write('some text')

        with open(self.state_file, 'w+') as fd:
            fd.write('{"Md5" : "' + dummy_md5 + '", "Command" : "run", "Cwd" : "dir"}')

        os.chdir('data')
        System.hardlink(os.path.join('..', self.cache_file), 'file.txt')
        os.chdir('..')
        pass

    def test_data_file_factory(self):
        data_path = self.path_factory.data_item(self.data_file)
        self.assertEqual(data_path.data.dvc, self.data_file)

        indirect_file_path = os.path.join('..', self._proj_dir, self.data_file)
        data_path_indirect = self.path_factory.data_item(indirect_file_path)
        self.assertEqual(data_path_indirect.data.dvc, self.data_file)
        pass

    def test_data_hardlink_factory(self):
        data_path = self.path_factory.existing_data_item(self.data_file)
        self.assertEqual(data_path.cache.dvc, self.cache_file)
        pass

    def test_data_hardlink_factory_cache(self):
        data_path = self.path_factory.existing_data_item(self.data_file)
        self.assertEqual(data_path.data.dvc, self.data_file)
        pass

    def test_data_hardlink_factory_exception(self):
        with self.assertRaises(DataItemError):
            self.path_factory.existing_data_item(self.cache_file)
        pass

    def test_data_path(self):
        file = os.path.join('data', 'file1')
        path = self.path_factory.path(file)
        self.assertEqual(path.dvc, file)
        self.assertEqual(path.relative, file)
        self.assertTrue(path.abs.endswith(file))


class TestDataPathInDataDir(BasicEnvironment):
    def setUp(self):
        BasicEnvironment.init_environment(self, os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'))

        git = GitWrapperI(git_dir=self._test_git_dir, commit='eeeff8f')
        self.data_dir = 'da'
        config = ConfigI()
        self.path_factory = PathFactory(git, config)

        deep_path = os.path.join(self.data_dir, 'dir1', 'd2', 'file.txt')
        self.cache_file = os.path.join(self._test_git_dir, ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR, 'dir1', 'd2', 'file.txt_eeeff8f')
        self.data_path = self.path_factory.data_item(deep_path, cache_file=self.cache_file)
        pass

    def test_data_file(self):
        target = os.path.join(self._test_git_dir, 'da', 'dir1', 'd2', 'file.txt')
        self.assertEqual(self.data_path.data.abs, target)

    def test_cache_file(self):
        self.assertEqual(self.data_path.cache.abs, self.cache_file)

    def test_state_file(self):
        target = os.path.join(self._test_git_dir, ConfigI.CONFIG_DIR, ConfigI.STATE_DIR, 'da', 'dir1', 'd2', 'file.txt.state')
        self.assertEqual(self.data_path.state.abs, target)


class TestDataFileObjLongPath(BasicEnvironment):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp', 'ntx_unit_test', 'proj', 'mydir', 'dd3')

        BasicEnvironment.init_environment(self,
                                          os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                                          curr_dir)

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        self._config = ConfigI('data')
        self.path_factory = PathFactory(self._git, self._config)

        self.cache_file = os.path.join(self._test_git_dir, ConfigI.CONFIG_DIR, ConfigI.CACHE_DIR, 'file1.txt_123ed8')
        self.data_path = self.path_factory.data_item(os.path.join('..', '..', 'data', 'file1.txt'), cache_file=self.cache_file)
        pass

    def test_data_file(self):
        target = os.path.join(self._test_git_dir, 'data', 'file1.txt')
        self.assertEqual(self.data_path.data.abs, target)

    def test_cache_file(self):
        self.assertEqual(self.data_path.cache.abs, self.cache_file)

    def test_state_file(self):
        target = os.path.join(self._test_git_dir, ConfigI.CONFIG_DIR, ConfigI.STATE_DIR, 'data', 'file1.txt.state')
        self.assertEqual(self.data_path.state.abs, target)


class RunOutsideGitRepoTest(BasicEnvironment):
    def setUp(self):
        curr_dir = os.path.join(os.path.sep, 'tmp')

        BasicEnvironment.init_environment(self,
                                          os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                                          curr_dir)

    def test_outside_git_dir(self):
        git = GitWrapperI(git_dir=self._test_git_dir, commit='123ed8')
        config = ConfigI('data')
        path_factory = PathFactory(git, config)

        with self.assertRaises(DataDirError):
            path_factory.data_item('file.txt')
        pass
