import os
from unittest import TestCase

from dvc.config import ConfigI
from dvc.git_wrapper import GitWrapperI
from dvc.path.data_item import DataItem
from dvc.path.factory import PathFactory
from dvc.settings import Settings
from dvc.utils import rmtree


class BasicEnvironment(TestCase):
    def init_environment(self, test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'), curr_dir=None):
        self._test_dir = os.path.realpath(test_dir)
        self._proj_dir = 'proj'
        self._test_git_dir = os.path.join(self._test_dir, self._proj_dir)
        self._old_curr_dir_abs = os.path.realpath(os.curdir)

        rmtree(self._test_dir)

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
        if self._old_curr_dir_abs:
            os.chdir(self._old_curr_dir_abs)
        if self._test_git_dir:
            rmtree(self._test_dir)
        pass


class DirHierarchyEnvironment(BasicEnvironment):
    def init_environment(self,
                         test_dir=os.path.join(os.path.sep, 'tmp', 'ntx_unit_test'),
                         curr_dir=None,
                         commit='abc12345'):
        ''' Creates data environment with data, cache and state dirs.
        data/
            file1.txt     --> ../cache/file1.txt_abc123
            dir1/
                file2.txt      --> ../../cache/dir1/file2.txt_abc123
                file3.txt      --> ../../cache/dir1/file3.txt_abc123
                dir11/
                    file4.txt  --> ../../../cache/dir1/dir11/file4.txt_abc123
            dir2/
                file5.txt      --> ../../cache/dir2/file5.txt_abc123
                file6.txt      --> an actual file
        '''

        BasicEnvironment.init_environment(self, test_dir, curr_dir)

        self._commit = commit

        self._git = GitWrapperI(git_dir=self._test_git_dir, commit=self._commit)
        self._config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(self._git, self._config)
        self.settings = Settings([], self._git, self._config)

        self.dir1 = os.path.join('dir1')
        self.dir11 = os.path.join('dir1/dir11')
        self.dir2 = os.path.join('dir2')

        os.mkdir('cache')
        os.mkdir('state')
        self.create_dirs(self.dir1)
        self.create_dirs(self.dir11)
        self.create_dirs(self.dir2)

        self.file1, self.cache1, self.state1 = self.crate_data_item('file1.txt')
        self.file2, self.cache2, self.state2 = self.crate_data_item(os.path.join(self.dir1, 'file2.txt'))
        self.file3, self.cache3, self.state3 = self.crate_data_item(os.path.join(self.dir1, 'file3.txt'))
        self.file4, self.cache4, self.state4 = self.crate_data_item(os.path.join(self.dir11, 'file4.txt'))
        self.file5, self.cache5, self.state5 = self.crate_data_item(os.path.join(self.dir2, 'file5.txt'))
        self.file6, self.cache6, self.state6 = self.crate_data_item(os.path.join(self.dir2, 'file6.txt'),
                                                                    cache_file=False)
        pass

    @staticmethod
    def create_content_file(file, content='some test'):
        fd = open(file, 'w+')
        fd.write(content)
        fd.close()

    def crate_data_item(self, data_file, cache_file=True, content='random text'):
        file_result = os.path.join('data', data_file)
        state_result = os.path.join('state', data_file + DataItem.STATE_FILE_SUFFIX)

        self.create_content_file(state_result, 'state content')

        if cache_file:
            cache_result = os.path.join('cache', data_file + DataItem.CACHE_FILE_SEP + self._commit)
            self.create_content_file(cache_result, content)

            relevant_dir = self.relevant_dir(data_file)
            os.symlink(os.path.join(relevant_dir, cache_result), file_result)
        else:
            cache_result = None
            self.create_content_file(file_result, content)

        return file_result, cache_result, state_result

    @staticmethod
    def relevant_dir(data_file):
        deepth = data_file.count(os.path.sep)
        relevant_path = '..'
        for i in range(deepth):
            relevant_path = os.path.join(relevant_path, '..')
        return relevant_path

    def create_dirs(self, dir):
        os.mkdir(os.path.join('data', dir))
        os.mkdir(os.path.join('cache', dir))
        os.mkdir(os.path.join('state', dir))
