import pytest

from dvc.dependency.hdfs import HDFSDependency
from tests import PY39, PYARROW_NOT_AVAILABLE
from tests.unit.dependency.test_local import TestLocalDependency


@pytest.mark.skipif(PY39, reason=PYARROW_NOT_AVAILABLE)
class TestHDFSDependency(TestLocalDependency):
    def _get_cls(self):
        return HDFSDependency
