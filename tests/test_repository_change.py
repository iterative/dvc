import os
import shutil
import subprocess
import tempfile
from unittest import TestCase

from dvc.config import ConfigI
from dvc.path.factory import PathFactory
from dvc.git_wrapper import GitWrapperI
from dvc.repository_change import RepositoryChange
from dvc.settings import Settings
from dvc.utils import rmtree


class BasicTestRepositoryChange(TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self._old_curr_dir_abs = os.path.realpath(os.curdir)

        self.tearDown()
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        os.mkdir('data')

        self._devnull = open(os.devnull, 'w')
        subprocess.Popen(['git', 'init'], stdout=self._devnull, stderr=None).wait()

        self.git = GitWrapperI(self.test_dir)
        self.config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(self.git, self.config)
        self.settings = Settings([], self.git, self.config)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        os.chdir(self._old_curr_dir_abs)

    @staticmethod
    def create_file(file2):
        fd = open(file2, 'w+')
        fd.write('random text')
        fd.close()


class TestRepositoryChange(BasicTestRepositoryChange):
    def test_all(self):
        file_to_modify = os.path.join('data', 'file1.txt')
        file_created_before_run = os.path.join('data', 'file2.txt')
        file_created_by_run = os.path.join('data', 'out.txt')
        file_to_remove = os.path.join('data', 'file3.txt')

        self.create_file(file_to_modify)
        self.create_file(file_created_before_run)
        self.create_file(file_to_remove)

        subprocess.Popen(['git', 'add', file_to_modify, file_to_remove],
                         stdout=self._devnull, stderr=None).wait()
        subprocess.Popen(['git', 'commit', '-m', '"Adding one file3"'],
                         stdout=self._devnull, stderr=None).wait()
        rmtree(file_to_remove)

        change = RepositoryChange(['ls', '-la'],
                                  self.settings,
                                  file_created_by_run,
                                  file_to_modify)

        expected_new = [file_created_by_run, file_created_before_run]
        new_file_abs = [x.data.dvc for x in change.new_data_items]
        self.assertEqual(set(new_file_abs), set(expected_new))

        self.assertEqual(len(change.removed_data_items), 1)
        self.assertEqual(change.removed_data_items[0].data.dvc, file_to_remove)
        self.assertEqual(len(change.modified_data_items), 1)
        self.assertEqual(change.modified_data_items[0].data.dvc, file_to_modify)

        expected_to_change_set = set(expected_new + [file_to_modify])
        changed_file_names = [x.data.dvc for x in change.changed_data_items]
        self.assertEqual(set(changed_file_names), expected_to_change_set)

        self.assertEqual([], change.externally_created_files)
        pass


class TestRepositoryChangeDeepInDirectory(BasicTestRepositoryChange):
    def test(self):
        os.mkdir(os.path.join('data', 'dir32'))
        deep_file = os.path.join('data', 'dir32', 'file1.txt')
        self.create_file(deep_file)

        change = RepositoryChange(['ls', '-la'],
                                  self.settings,
                                  deep_file,
                                  None)

        self.assertEqual([x.data.dvc for x in change.new_data_items], [deep_file])


class TestRepositoryChangeExternallyCreated(BasicTestRepositoryChange):
    def test(self):
        not_in_data_dir_file = 'file1.txt'
        self.create_file(not_in_data_dir_file)

        change = RepositoryChange(['ls', '-la'],
                                  self.settings,
                                  not_in_data_dir_file,
                                  None)

        self.assertEqual([os.path.realpath(not_in_data_dir_file)],
                         change.externally_created_files)
