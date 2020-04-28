from dvc.output.hdfs import HDFSOutput
from tests.unit.output.test_local import TestLocalOutput


class TestHDFSOutput(TestLocalOutput):
    def _get_cls(self):
        return HDFSOutput
