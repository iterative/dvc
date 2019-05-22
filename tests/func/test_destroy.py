import os

from dvc.main import main

from tests.func.test_repro import TestRepro


class TestDestroyNoConfirmation(TestRepro):
    def test(self):
        ret = main(["destroy"])
        self.assertNotEqual(ret, 0)


class TestDestroyForce(TestRepro):
    def test(self):
        ret = main(["destroy", "-f"])
        self.assertEqual(ret, 0)

        self.assertFalse(os.path.exists(self.dvc.dvc_dir))
        self.assertFalse(os.path.exists(self.file1_stage))
        self.assertFalse(os.path.exists(self.file1))
