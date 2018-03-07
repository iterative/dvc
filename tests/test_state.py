import os
import stat

from dvc.system import System
from dvc.state import State, StateEntry
from dvc.utils import file_md5

from tests.basic_env import TestDvc


class TestState(TestDvc):
    def test_update(self):
        path = os.path.join(self.dvc.root_dir, self.FOO)
        md5 = file_md5(path)[0]
        mtime = os.path.getmtime(path)
        inode = System.inode(path)

        state = State(self.dvc.root_dir, self.dvc.dvc_dir)

        state.update(path)
        entry = state.get(path)
        self.assertIsInstance(entry, StateEntry)
        self.assertEqual(entry.md5, md5)
        self.assertEqual(entry.mtime, mtime)
        self.assertEqual(entry.inode, inode)

        os.chmod(path, stat.S_IWRITE)
        os.unlink(path)
        with open(path, 'w+') as fd:
            fd.write('1')

        md5 = file_md5(path)[0]
        mtime = os.path.getmtime(path)
        inode = System.inode(path)

        entry = state.update(path)
        self.assertIsInstance(entry, StateEntry)
        self.assertEqual(entry.md5, md5)
        self.assertEqual(entry.mtime, mtime)
        self.assertEqual(entry.inode, inode)
