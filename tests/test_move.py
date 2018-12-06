import os

from dvc.main import main
from dvc.exceptions import DvcException, MoveNotDataSourceError

from tests.basic_env import TestDvc
from tests.test_repro import TestRepro


class TestMove(TestDvc):
    def test(self):
        dst = self.FOO + '1'
        self.dvc.add(self.FOO)
        self.dvc.move(self.FOO, dst)

        self.assertFalse(os.path.isfile(self.FOO))
        self.assertTrue(os.path.isfile(dst))


class TestMoveNonExistentFile(TestDvc):
    def test(self):
        with self.assertRaises(DvcException):
            self.dvc.move('non_existent_file', 'dst')


class TestMoveDirectory(TestDvc):
    def test(self):
        dst = 'dst'
        stages = self.dvc.add(self.DATA_DIR)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)
        self.dvc.move(self.DATA_DIR, dst)
        self.assertFalse(os.path.exists(self.DATA_DIR))
        self.assertTrue(os.path.exists(dst))


class TestCmdMove(TestDvc):
    def test(self):
        stages = self.dvc.add(self.FOO)
        self.assertEqual(len(stages), 1)
        self.assertTrue(stages[0] is not None)

        ret = main(['move', self.FOO, self.FOO + '1'])
        self.assertEqual(ret, 0)

        ret = main(['move', 'non-existing-file', 'dst'])
        self.assertNotEqual(ret, 0)


class TestMoveNotDataSource(TestRepro):
    def test(self):
        from dvc.project import Project

        self.dvc = Project(self._root_dir)
        with self.assertRaises(MoveNotDataSourceError):
            self.dvc.move(self.file1, 'dst')

        ret = main(['move', self.file1, 'dst'])
        self.assertNotEqual(ret, 0)
