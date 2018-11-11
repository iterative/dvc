import os
import stat
import time

from dvc.system import System
from dvc.state import State
from dvc.utils import file_md5
from dvc.main import main

from tests.basic_env import TestDvc


class TestState(TestDvc):
    def test_update(self):
        path = os.path.join(self.dvc.root_dir, self.FOO)
        md5 = file_md5(path)[0]
        mtime = os.path.getmtime(path)
        inode = System.inode(path)

        state = State(self.dvc, self.dvc.config._config)

        with state:
            entry_md5 = state.update(path)
            self.assertEqual(entry_md5, md5)

            # Sleep some time to simulate realistic behavior.
            # Some filesystems have a bad date resolution for
            # mtime(i.e. 1sec for HFS) that cause problems with
            # our 'state' system not being able to distinguish
            # files that were modified within that delta.
            time.sleep(1)        

            os.unlink(path)
            with open(path, 'w+') as fd:
                fd.write('1')

            md5 = file_md5(path)[0]
            mtime = os.path.getmtime(path)
            inode = System.inode(path)

            entry_md5 = state.update(path)
            self.assertEqual(entry_md5, md5)


class TestStateOverflow(TestDvc):
    def test(self):
        # NOTE: trying to add more entries than state can handle,
        # to see if it will clean up and vacuum successfully
        ret = main(['config', 'state.row_limit', '10'])
        self.assertEqual(ret, 0)

        dname = 'dir'
        os.mkdir(dname)
        for i in range(20):
            with open(os.path.join(dname, str(i)), 'w+') as fobj:
                fobj.write(str(i))

        ret = main(['add', 'dir'])
        self.assertEqual(ret, 0)
