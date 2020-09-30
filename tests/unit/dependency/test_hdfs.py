import pytest

from dvc.dependency.hdfs import HDFSDependency
from tests import HDFS_SKIP_REASON, PY39
from tests.unit.dependency.test_local import TestLocalDependency


@pytest.mark.skipif(PY39, reason=HDFS_SKIP_REASON)
class TestHDFSDependency(TestLocalDependency):
    def _get_cls(self):
        return HDFSDependency
