import glob, os

from dvc.command.gc import CmdGC
from dvc.config import ConfigI
from dvc.git_wrapper import GitWrapperI
from dvc.path.data_item import DataItem
from dvc.path.factory import PathFactory
from dvc.settings import Settings
from dvc.system import System
from tests.basic_env import BasicEnvironment


class TestCmdDataRemove(BasicEnvironment):
    def setUp(self):
        self._test_dir = os.path.join(os.path.sep, 'tmp', 'dvc_unit_test')
        curr_dir = None
        commit = 'abc1234'

        BasicEnvironment.init_environment(self, self._test_dir, curr_dir)

        self._commit = commit

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit=self._commit)
        self._config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(self._git, self._config)
        self.settings = Settings([], self._git, self._config)

        self.dir1 = 'dir1'

        self.create_dirs(self.dir1)

    def test_single_file(self):
        os.chdir(self._test_git_dir)

        self.crate_data_item_with_five_caches('', 'file1.txt')
        self.assertEqual(5, self.cache_file_nums('file1*'))

        cmd = CmdGC(self.settings)
        os.chdir(self._test_git_dir)

        cmd.gc_file(os.path.join('data', 'file1.txt'))
        self.assertEqual(1, self.cache_file_nums('file1*'))

    def test_all(self):
        os.chdir(self._test_git_dir)

        self.crate_data_item_with_five_caches('', 'file1.txt')
        self.crate_data_item_with_five_caches('', 'file2.txt')
        self.crate_data_item_with_five_caches(self.dir1, 'file3.txt')

        self.assertEqual(5, self.cache_file_nums('file1*'))
        self.assertEqual(5, self.cache_file_nums('file2*'))
        self.assertEqual(5, self.cache_file_nums(os.path.join(self.dir1, 'file3*')))

        cmd = CmdGC(self.settings)
        os.chdir(self._test_git_dir)

        cmd.parsed_args.no_git_actions = True
        cmd.parsed_args.target = ['data']
        cmd.gc_all()

        self.assertEqual(1, self.cache_file_nums('file1*'))
        self.assertEqual(1, self.cache_file_nums('file2*'))
        self.assertEqual(1, self.cache_file_nums(os.path.join(self.dir1, 'file3*')))

    def test_empty(self):
        os.chdir(self._test_git_dir)

        self.crate_data_item_with_five_caches('', 'file1.txt')
        self.crate_data_item_with_five_caches('', 'file2.txt')
        self.assertEqual(5, self.cache_file_nums('file1*'))
        self.assertEqual(5, self.cache_file_nums('file2*'))

        cmd = CmdGC(self.settings)
        os.chdir(self._test_git_dir)

        cmd.parsed_args.no_git_actions = True
        cmd.gc_all()

        self.assertEqual(5, self.cache_file_nums('file1*'))
        self.assertEqual(5, self.cache_file_nums('file2*'))

    def create_dirs(self, dir):
        os.mkdir(os.path.join(self._test_git_dir, 'data', dir))
        os.mkdir(os.path.join(self._test_git_dir, 'cache', dir))
        os.mkdir(os.path.join(self._test_git_dir, 'state', dir))

    def cache_file_nums(self, pattern):
        os.chdir(os.path.join(self._test_git_dir, 'cache'))
        files = []
        for file in glob.glob(pattern):
            files.append(file)
        return len(files)

    def crate_data_item_with_five_caches(self, dir, data_file, content='random text'):
        os.chdir(self._test_git_dir)

        file_result = os.path.join('data', dir, data_file)

        state_result = os.path.join('state', dir, data_file + DataItem.STATE_FILE_SUFFIX)
        self.create_content_file(state_result, 'state content')

        file_prefix = data_file + DataItem.CACHE_FILE_SEP
        cache_result = os.path.join('cache', dir, file_prefix + self._commit)
        self.create_content_file(cache_result, content)
        relevant_dir = self.relevant_dir(os.path.join(dir, data_file))
        cache_file = os.path.join(relevant_dir, cache_result)

        d = os.path.join(self._test_git_dir, 'data', dir)
        os.chdir(d)
        print('---- CREATE SL {}: {} --> {}'.format(d, data_file, cache_file))
        System.symlink(cache_file, data_file)
        os.chdir(self._test_git_dir)

        data_item = self.settings.path_factory.existing_data_item(file_result)
        print('*DATA ITEM {}: {} {}'.format(file_result, data_item.data.relative, data_item.cache.relative))

        print('----> {} {} {} {}'.format(dir, data_file, cache_file, file_result))

        # Additional cache files
        self.create_content_file(os.path.join('cache', dir, file_prefix + 'aaaaaaa'))
        self.create_content_file(os.path.join('cache', dir, file_prefix + '1111111'))
        self.create_content_file(os.path.join('cache', dir, file_prefix + '5555555'))
        self.create_content_file(os.path.join('cache', dir, file_prefix + '123abff'))
        return

    @staticmethod
    def create_content_file(file, content='some test'):
        fd = open(file, 'w+')
        fd.write(content)
        fd.close()

    @staticmethod
    def relevant_dir(data_file):
        deepth = data_file.count(os.path.sep)
        relevant_path = '..'
        for i in range(deepth):
            relevant_path = os.path.join(relevant_path, '..')
        return relevant_path
