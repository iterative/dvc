import os
import shutil
import subprocess
from unittest import TestCase

from dvc.config import ConfigI
from dvc.path.factory import PathFactory
from dvc.git_wrapper import GitWrapperI
from dvc.repository_change import RepositoryChange


class TestRepositoryChange(TestCase):
    def setUp(self):
        self.test_dir = os.path.realpath(os.path.join('/', 'tmp', 'dvc_unit_test_repo_change'))

        self.tearDown()
        os.mkdir(self.test_dir)
        os.chdir(self.test_dir)
        os.mkdir('data')

        devnull = open(os.devnull, 'w')
        subprocess.Popen(['git', 'init'], stdout=devnull, stderr=None).wait()

        self.file_to_modify = 'file1.txt'
        self.file_created_before_run = 'file2.txt'
        self.file_created_by_run = os.path.join('data', 'out.txt')
        self.file_to_remove = 'file3.txt'

        self.create_file(self.file_to_modify)
        self.create_file(self.file_created_before_run)
        self.create_file(self.file_to_remove)

        subprocess.Popen(['git', 'add', self.file_to_modify, self.file_to_remove],
                         stdout=devnull, stderr=None).wait()
        subprocess.Popen(['git', 'commit', '-m', '"Adding one file3"'],
                         stdout=devnull, stderr=None).wait()
        os.remove(self.file_to_remove)

        self.git = GitWrapperI(self.test_dir)
        self.config = ConfigI('data', 'cache', 'state')
        self.path_factory = PathFactory(self.git, self.config)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)
        pass

    @staticmethod
    def create_file(file2):
        fd = open(file2, 'w+')
        fd.write('random text')
        fd.close()

    def test_all(self):
        change = RepositoryChange(['ls', '-la'],
                                  self.file_created_by_run,
                                  self.file_to_modify,
                                  self.git,
                                  self.config,
                                  self.path_factory)

        expected_new = [self.file_created_by_run, self.file_created_before_run]
        self.assertEqual(set(change.new_files), set(map(os.path.realpath, expected_new)))

        self.assertEqual(change.removed_files, [os.path.realpath(self.file_to_remove)])
        self.assertEqual(change.modified_files, [os.path.realpath(self.file_to_modify)])

        expected_to_change_set = set(map(os.path.realpath, expected_new + [self.file_to_modify]))
        self.assertEqual(set(change.changed_files), expected_to_change_set)

        expected_to_change_set.remove(os.path.realpath(self.file_created_by_run))
        self.assertEqual(expected_to_change_set, set(change.externally_created_files))

        self.assertEqual(1, len(change.data_items_for_changed_files))
        self.assertEqual(self.file_created_by_run, change.data_items_for_changed_files[0].data.dvc)
        pass
