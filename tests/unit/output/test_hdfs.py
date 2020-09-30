import pytest

from dvc.output.hdfs import HDFSOutput
from tests import HDFS_SKIP_REASON, PY39
from tests.unit.output.test_local import TestLocalOutput


@pytest.mark.skipif(PY39, reason=HDFS_SKIP_REASON)
class TestHDFSOutput(TestLocalOutput):
    def _get_cls(self):
        return HDFSOutput
