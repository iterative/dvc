from dvc.state import State
from tests.basic_env import TestDvc


class TestMtimeAndSizeReturnType(TestDvc):
    def test(self):
        file_time, file_size = State.mtime_and_size(self.DATA)
        dir_time, dir_size = State.mtime_and_size(self.DATA_DIR)

        self.assertIs(type(file_time), str)
        self.assertIs(type(file_size), str)
        self.assertIs(type(dir_time), str)
        self.assertIs(type(dir_size), str)
