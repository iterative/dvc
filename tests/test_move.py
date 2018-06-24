import os

from dvc.main import main
from dvc.exceptions import DvcException

from tests.basic_env import TestDvc


class TestMove(TestDvc):
    def test(self):
        dst = self.FOO + '1'
        self.dvc.add(self.FOO)
        self.dvc.move(self.FOO, dst)

        self.assertFalse(os.path.isfile(self.FOO))
        self.assertTrue(os.path.isfile(dst))


class TestMoveNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(DvcException) as cx:
            self.dvc.move('non_existent_file', 'dst')


class TestMoveDirectory(TestDvc):
    def test(self):
        dst = 'dst'
        stage_add = self.dvc.add(self.DATA_DIR)
        self.dvc.move(self.DATA_DIR, dst)
        self.assertFalse(os.path.exists(self.DATA_DIR))
        self.assertTrue(os.path.exists(dst))


class TestCmdMove(TestDvc):
    def test(self):
        stage = self.dvc.add(self.FOO)
        ret = main(['move', self.FOO, self.FOO + '1'])
        self.assertEqual(ret, 0)

        ret = main(['move', 'non-existing-file', 'dst'])
        self.assertNotEqual(ret, 0)
