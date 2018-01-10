import os
import stat
import shutil
import filecmp

from dvc.main import main
from dvc.data_cloud import file_md5
from dvc.stage import Stage, OutputNoCacheError, OutputOutsideOfRepoError
from dvc.stage import OutputDoesNotExistError, OutputIsNotFileError
from dvc.project import StageNotFoundError

from tests.test_repro import TestRepro


class TestCheckout(TestRepro):
    def setUp(self):
        super(TestCheckout, self).setUp()

        self.orig = 'orig'
        shutil.copy(self.FOO, self.orig)
        os.chmod(self.FOO, stat.S_IWRITE)
        os.unlink(self.FOO)

    def test(self):
        self.dvc.checkout()
        self._test_checkout()

    def _test_checkout(self):
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertTrue(filecmp.cmp(self.FOO, self.orig))


class TestCmdCheckout(TestCheckout):
    def test(self):
        ret = main(['checkout'])
        self.assertEqual(ret, 0)
        self._test_checkout()
