import os
import sys

import pytest

from dvc.dependency.hdfs import DependencyHDFS
from tests.unit.dependency.test_local import TestDependencyLOCAL


@pytest.mark.skipif(
    sys.version_info[0] == 2 and os.name == "nt",
    reason="Not supported for python 2 on Windows.",
)
class TestDependencyHDFS(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencyHDFS
