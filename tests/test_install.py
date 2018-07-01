import os

from dvc.main import main

from tests.basic_env import TestDvc


class TestInstall(TestDvc):
    def test(self):
        ret = main(['install'])
        self.assertEqual(ret, 0)

        ret = main(['install'])
        self.assertNotEqual(ret, 0)

        self.assertTrue(os.path.isfile('.git/hooks/post-checkout'))

        self.dvc.add(self.FOO)
        os.unlink(self.FOO)

        self.dvc.scm.checkout('branch', create_new=True)
        self.assertTrue(os.path.isfile(self.FOO))
