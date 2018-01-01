import os

from dvc.state import State, StateEntry

from tests.basic_env import TestDvc


class TestState(TestDvc):
    def test_update(self):
        path = os.path.join(self.dvc.root_dir, '1')
        md5_1 = '1'
        md5_2 = '2'
        mtime_1 = 1
        mtime_2 = 2

        state = State(self.dvc.root_dir, self.dvc.dvc_dir)
        self.assertIsNone(state.get(path))

        state.add(path, md5_1, mtime_1)
        entry = state.get(path)
        self.assertIsInstance(entry, StateEntry)
        self.assertEqual(entry.path, path)
        self.assertEqual(entry.md5, md5_1)
        self.assertEqual(entry.mtime, mtime_1)

        state.update(path, md5_2, mtime_2)
        entry = state.get(path)
        self.assertIsInstance(entry, StateEntry)
        self.assertEqual(entry.path, path)
        self.assertEqual(entry.md5, md5_2)
        self.assertEqual(entry.mtime, mtime_2)
