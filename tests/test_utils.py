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

    def test_file_md5_crlf(self):
        with open('cr', 'wb+') as fd:
            fd.write(b'a\nb\nc')
        with open('crlf', 'wb+') as fd:
            fd.write(b'a\r\nb\r\nc')

        self.assertEqual(utils.file_md5('cr')[0], utils.file_md5('crlf')[0])
