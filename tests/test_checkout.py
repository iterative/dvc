import os
import shutil
import filecmp

from dvc.data_cloud import file_md5
from dvc.stage import Stage, OutputNoCacheError, OutputOutsideOfRepoError
from dvc.stage import OutputDoesNotExistError, OutputIsNotFileError
from dvc.project import StageNotFoundError

from tests.basic_env import TestDvc


class TestCheckout(TestDvc):
    def test_checkout(self):
        cmd = 'python {} {} {}'.format(self.CODE, self.FOO, 'out')
        deps = [self.FOO]
        deps_no_cache = [self.CODE]
        outs = [os.path.join(self.dvc.root_dir, 'out')]
        outs_no_cache = []
        locked = False
        fname = os.path.join(self.dvc.root_dir, 'out.dvc')
        cwd = os.curdir

        stage = self.dvc.run(cmd, deps, deps_no_cache, outs, outs_no_cache, locked, fname, cwd)

        shutil.copy(self.FOO, 'orig')
        os.unlink(self.FOO)
        self.dvc.checkout()
        self.assertTrue(os.path.isfile(self.FOO))
        self.assertTrue(filecmp.cmp(self.FOO, 'orig'))
