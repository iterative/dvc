import os
import sys
import unittest

from dvc.main import main

from tests.basic_env import TestDvc


@unittest.skipIf(
    sys.platform == "win32", "Git hooks aren't supported on Windows"
)
class TestInstall(TestDvc):
    def test(self):
        ret = main(["install"])
        self.assertEqual(ret, 0)

        ret = main(["install"])
        self.assertNotEqual(ret, 0)

        def hook(name):
            return os.path.join(".git", "hooks", name)

        self.assertTrue(os.path.isfile(hook("post-checkout")))
        self.assertTrue(os.path.isfile(hook("pre-commit")))

        self.dvc.add(self.FOO)
        self.dvc.scm.add([".gitignore", self.FOO + ".dvc"])
        self.dvc.scm.commit("add")
        os.unlink(self.FOO)

        self.dvc.scm.checkout("branch", create_new=True)
        self.assertTrue(os.path.isfile(self.FOO))
