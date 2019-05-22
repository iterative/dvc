from dvc.output.hdfs import OutputHDFS

from tests.unit.output.test_local import TestOutputLOCAL


class TestOutputHDFS(TestOutputLOCAL):
    def _get_cls(self):
        return OutputHDFS
