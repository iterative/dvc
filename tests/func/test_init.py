import os

from dvc.main import main
from dvc.repo import Repo as DvcRepo
from dvc.exceptions import InitError

from tests.basic_env import TestGit, TestDir


class TestInit(TestGit):
    def _test_init(self):
        self.assertTrue(os.path.exists(DvcRepo.DVC_DIR))
        self.assertTrue(os.path.isdir(DvcRepo.DVC_DIR))

    def test_api(self):
        DvcRepo.init()

        self._test_init()

    def test_cli(self):
        ret = main(["init"])
        self.assertEqual(ret, 0)

        self._test_init()


class TestDoubleInit(TestInit):
    def test(self):
        ret = main(["init"])
        self.assertEqual(ret, 0)
        self._test_init()

        ret = main(["init"])
        self.assertNotEqual(ret, 0)
        self._test_init()

        ret = main(["init", "--force"])
        self.assertEqual(ret, 0)
        self._test_init()


class TestInitNoSCMFail(TestDir):
    def test_api(self):
        with self.assertRaises(InitError):
            DvcRepo.init()

    def test_cli(self):
        ret = main(["init"])
        self.assertNotEqual(ret, 0)


class TestInitNoSCM(TestDir):
    def _test_init(self):
        self.assertTrue(os.path.exists(DvcRepo.DVC_DIR))
        self.assertTrue(os.path.isdir(DvcRepo.DVC_DIR))

    def test_api(self):
        DvcRepo.init(no_scm=True)

        self._test_init()

    def test_cli(self):
        ret = main(["init", "--no-scm"])
        self.assertEqual(ret, 0)

        self._test_init()


def test_init_quiet_should_not_display_welcome_screen(git, caplog):
    ret = main(["init", "--quiet"])

    assert 0 == ret
    assert "" == caplog.text
