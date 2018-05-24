import os
import stat
import shutil
import filecmp

from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage
from dvc.output.base import OutputAlreadyTrackedError
from dvc.output.base import OutputDoesNotExistError, OutputIsNotFileOrDirError
from dvc.project import StageNotFoundError
from dvc.command.add import CmdAdd

from tests.basic_env import TestDvc


class TestAdd(TestDvc):
    def test(self):
        md5 = file_md5(self.FOO)[0]

        stage = self.dvc.add(self.FOO)

        self.assertIsInstance(stage, Stage)
        self.assertTrue(os.path.isfile(stage.path))
        self.assertEqual(len(stage.outs), 1)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(stage.cmd, None)
        self.assertEqual(stage.outs[0].md5, md5)


class TestAddNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(OutputDoesNotExistError) as cx:
            self.dvc.add('non_existent_file')


class TestAddDirectory(TestDvc):
    def test(self):
        dname = 'directory'
        os.mkdir(dname)
        self.create(os.path.join(dname, 'file'), 'file')
        self.dvc.add(dname)

class TestAddDirectoryWithForwardSlash(TestDvc):
    def test(self):
        dname = 'directory/'
        os.mkdir(dname)
        self.create(os.path.join(dname, 'file'), 'file')
        stage = self.dvc.add(dname)
        self.assertEquals(os.path.abspath('directory.dvc'), stage.path)

class TestAddTrackedFile(TestDvc):
    def test(self):
        fname = 'tracked_file'
        self.create(fname, 'tracked file contents')
        self.dvc.scm.add([fname])
        self.dvc.scm.commit('add {}'.format(fname))

        with self.assertRaises(OutputAlreadyTrackedError) as cx:
            self.dvc.add(fname)


class TestAddDirWithExistingCache(TestDvc):
    def test(self):
        dname = 'a'
        fname = os.path.join(dname, 'b')
        os.mkdir(dname)
        shutil.copyfile(self.FOO, fname)

        self.dvc.add(self.FOO)
        self.dvc.add(dname)


class TestAddModifiedDir(TestDvc):
    def test(self):
        self.dvc.add(self.DATA_DIR)
        os.unlink(self.DATA)
        self.dvc.add(self.DATA_DIR)


class TestCmdAdd(TestDvc):
    def test(self):
        ret = main(['add',
                    self.FOO])
        self.assertEqual(ret, 0)

        ret = main(['add',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)
