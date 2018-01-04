import os
import shutil
import filecmp

from dvc.data_cloud import file_md5
from dvc.stage import Stage, OutputNoCacheError, OutputOutsideOfRepoError
from dvc.stage import OutputDoesNotExistError, OutputIsNotFileError
from dvc.stage import OutputAlreadyTrackedError
from dvc.project import StageNotFoundError

from tests.basic_env import TestDvc


class TestAdd(TestDvc):
    def test(self):
        md5 = file_md5(self.FOO)[0]

        stage = self.dvc.add(self.FOO)

        self.assertIsInstance(stage, Stage)
        self.assertTrue(os.path.isfile(stage.path))
        self.assertEqual(len(stage.outs), 1)
        self.assertEqual(len(stage.deps), 0)
        self.assertTrue(stage.locked)
        self.assertEqual(stage.cmd, None)
        self.assertEqual(stage.outs[0].md5, md5)


class TestAddNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(OutputDoesNotExistError) as cx:
            self.dvc.add('non_existent_file')


class TestAddFileOutsideOfRepo(TestDvc):
    def test(self):
        with self.assertRaises(OutputOutsideOfRepoError) as cx:
            self.dvc.add(os.path.join(os.path.dirname(self.dvc.root_dir), self.FOO))


class TestAddDirectory(TestDvc):
    def test(self):
        dname = 'directory'
        os.mkdir(dname)
        with self.assertRaises(OutputIsNotFileError) as cx:
            self.dvc.add(dname)


class TestAddTrackedFile(TestDvc):
    def test(self):
        fname = 'tracked_file'
        self.create(fname, 'tracked file contents')
        self.dvc.scm.add([fname])
        self.dvc.scm.commit('add {}'.format(fname))

        with self.assertRaises(OutputAlreadyTrackedError) as cx:
            self.dvc.add(fname)
