import os
import stat
import shutil
import filecmp

from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage
from dvc.exceptions import DvcException
from dvc.output.base import OutputAlreadyTrackedError
from dvc.output.base import OutputDoesNotExistError, OutputIsNotFileOrDirError
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
        self.assertEqual(stage.outs[0].info['md5'], md5)


class TestAddUnupportedFile(TestDvc):
    def test(self):
        with self.assertRaises(DvcException) as cx:
            self.dvc.add('unsupported://unsupported')


class TestAddDirectory(TestDvc):
    def test(self):
        dname = 'directory'
        os.mkdir(dname)
        self.create(os.path.join(dname, 'file'), 'file')
        stage = self.dvc.add(dname)
        self.assertNotEqual(stage, None)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(len(stage.outs), 1)

        md5 = stage.outs[0].info['md5']

        dir_info = self.dvc.cache.local.load_dir_cache(md5)
        for info in dir_info:
            self.assertTrue('\\' not in info['relpath'])


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


class TestAddFileInDir(TestDvc):
    def test(self):
        stage = self.dvc.add(self.DATA_SUB)
        self.assertNotEqual(stage, None)
        self.assertEqual(len(stage.deps), 0)
        self.assertEqual(len(stage.outs), 1)
        self.assertEqual(stage.relpath, self.DATA_SUB + '.dvc')


class TestCmdAdd(TestDvc):
    def test(self):
        ret = main(['add',
                    self.FOO])
        self.assertEqual(ret, 0)

        ret = main(['add',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)
