import os
import shutil
import filecmp

from dvc import utils
from tests.basic_env import TestDvc


class TestUtils(TestDvc):
    def test_copyfile(self):
        src = 'file1'
        dest = 'file2'
        dest_dir = 'testdir'

        with open(src, 'w+') as f:
            f.write('file1contents')

        os.mkdir(dest_dir)

        utils.copyfile(src, dest)
        self.assertTrue(filecmp.cmp(src, dest))

        utils.copyfile(src, dest_dir)
        self.assertTrue(filecmp.cmp(src, '{}/{}'.format(dest_dir, src)))

        shutil.rmtree(dest_dir)
        os.remove(src)
        os.remove(dest)
