from dvc.main import main
from dvc.stage import Stage

from tests.test_repro import TestRepro


class TestCmdLock(TestRepro):
    def test_lock(self):
        ret = main(['lock',
                    self.file1_stage])
        stage = Stage.load(self.dvc, self.file1_stage)
        self.assertEqual(ret, 0)
        self.assertTrue(stage.locked)

    def test_unlock(self):
        ret = main(['lock',
                    '-u',
                    self.file1_stage])
        stage = Stage.load(self.dvc, self.file1_stage)
        self.assertEqual(ret, 0)
        self.assertFalse(stage.locked)

    def test_twice(self):
        self.test_lock()
        self.test_lock()

        self.test_unlock()
        self.test_unlock()

    def test_non_existing(self):
        ret = main(['lock',
                    'non-existing-file'])
        self.assertNotEqual(ret, 0)
