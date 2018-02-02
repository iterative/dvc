import os
import shutil
import filecmp
from checksumdir import dirhash

from dvc.main import main
from dvc.utils import file_md5
from dvc.stage import Stage, CmdOutputNoCacheError, CmdOutputOutsideOfRepoError
from dvc.stage import CmdOutputDoesNotExistError, CmdOutputIsNotFileOrDirError
from dvc.stage import CmdOutputAlreadyTrackedError
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
        with self.assertRaises(CmdOutputDoesNotExistError) as cx:
            self.dvc.add('non_existent_file')


class TestAddFileOutsideOfRepo(TestDvc):
    def test(self):
        with self.assertRaises(CmdOutputOutsideOfRepoError) as cx:
            self.dvc.add(os.path.join(os.path.dirname(self.dvc.root_dir), self.FOO))


class TestAddDirectory(TestDvc):
    def test(self):
        dname = 'directory'
        os.mkdir(dname)
        self.create(os.path.join(dname, 'file'), 'file')
        self.dvc.add(dname)


class TestAddTrackedFile(TestDvc):
    def test(self):
        fname = 'tracked_file'
        self.create(fname, 'tracked file contents')
        self.dvc.scm.add([fname])
        self.dvc.scm.commit('add {}'.format(fname))

        with self.assertRaises(CmdOutputAlreadyTrackedError) as cx:
            self.dvc.add(fname)


class TestAddDirWithExistingCache(TestDvc):
    def test(self):
        dname = 'a'
        fname = os.path.join(dname, 'b')
        os.mkdir(dname)
        shutil.copyfile(self.FOO, fname)

        self.dvc.add(self.FOO)
        self.dvc.add(dname)


class TestCmdAdd(TestDvc):
    def test(self):
        ret = main(['add',
                    self.FOO])
        self.assertEqual(ret, 0)

        ret = main(['add',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)
