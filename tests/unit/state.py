from dvc.state import State
from tests.basic_env import TestDvc


class TestMtimeAndSizeReturnType(TestDvc):
    def test(self):
        self.create(self.DATA, self.DATA_CONTENTS)
        time, size = State.mtime_and_size(self.DATA)

        self.assertIs(type(time), str)
        self.assertIs(type(time), str)
