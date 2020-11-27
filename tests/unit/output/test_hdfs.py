import pytest

from dvc.output.hdfs import HDFSOutput
from tests import PY39, PYARROW_NOT_AVAILABLE
from tests.unit.output.test_local import TestLocalOutput


@pytest.mark.skipif(PY39, reason=PYARROW_NOT_AVAILABLE)
@pytest.mark.hdfs
class TestHDFSOutput(TestLocalOutput):
    def _get_cls(self):
        return HDFSOutput
