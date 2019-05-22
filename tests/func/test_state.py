import os
import mock
from dvc.path.local import PathLOCAL

from dvc.utils.compat import str

from dvc.state import State
from dvc.utils import file_md5
from dvc.main import main
from dvc.utils.fs import get_inode

from tests.basic_env import TestDvc


class TestState(TestDvc):
    def test_update(self):
        path = os.path.join(self.dvc.root_dir, self.FOO)
        path_info = PathLOCAL(path=path)
        md5 = file_md5(path)[0]

        state = State(self.dvc, self.dvc.config.config)

        with state:
            state.save(path_info, md5)
            entry_md5 = state.get(path_info)
            self.assertEqual(entry_md5, md5)

            os.unlink(path)
            with open(path, "a") as fd:
                fd.write("1")

            entry_md5 = state.get(path_info)
            self.assertTrue(entry_md5 is None)

            md5 = file_md5(path)[0]
            state.save(path_info, md5)

            entry_md5 = state.get(path_info)
            self.assertEqual(entry_md5, md5)


class TestStateOverflow(TestDvc):
    def test(self):
        # NOTE: trying to add more entries than state can handle,
        # to see if it will clean up and vacuum successfully
        ret = main(["config", "state.row_limit", "10"])
        self.assertEqual(ret, 0)

        dname = "dir"
        os.mkdir(dname)
        for i in range(20):
            with open(os.path.join(dname, str(i)), "w+") as fobj:
                fobj.write(str(i))

        ret = main(["add", "dir"])
        self.assertEqual(ret, 0)


class TestGetStateRecordForInode(TestDvc):
    @staticmethod
    def mock_get_inode(special_path, special_value):
        def get_inode_mocked(path):
            if path == special_path:
                return special_value
            else:
                return get_inode(path)

        return get_inode_mocked

    @mock.patch("dvc.state.get_inode", autospec=True)
    def test_transforms_inode(self, get_inode_mock):
        state = State(self.dvc, self.dvc.config.config)
        inode = state.MAX_INT + 2
        self.assertNotEqual(inode, state._to_sqlite(inode))

        path = os.path.join(self.dvc.root_dir, self.FOO)
        md5 = file_md5(path)[0]
        get_inode_mock.side_effect = self.mock_get_inode(path, inode)

        with state:
            state.save(PathLOCAL(path=path), md5)
            ret = state.get_state_record_for_inode(inode)
            self.assertIsNotNone(ret)
