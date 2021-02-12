import pytest

from dvc.output.hdfs import HDFSOutput
from tests.unit.output.test_local import TestLocalOutput


@pytest.mark.hdfs
class TestHDFSOutput(TestLocalOutput):
    def _get_cls(self):
        return HDFSOutput
