from dvc.utils.compat import str

import os

from dvc.state import State
from tests.basic_env import TestDvc


class TestMtimeAndSize(TestDvc):
    def test(self):
        file_time, file_size = State._mtime_and_size(self.DATA)
        dir_time, dir_size = State._mtime_and_size(self.DATA_DIR)

        actual_file_size = os.path.getsize(self.DATA)
        actual_dir_size = (
            os.path.getsize(self.DATA_DIR)
            + os.path.getsize(self.DATA)
            + os.path.getsize(self.DATA_SUB_DIR)
            + os.path.getsize(self.DATA_SUB)
        )

        self.assertIs(type(file_time), str)
        self.assertIs(type(file_size), str)
        self.assertEqual(file_size, str(actual_file_size))
        self.assertIs(type(dir_time), str)
        self.assertIs(type(dir_size), str)
        self.assertEqual(dir_size, str(actual_dir_size))
