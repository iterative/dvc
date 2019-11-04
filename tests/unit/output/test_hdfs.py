import os
import sys

import pytest

from dvc.output.hdfs import OutputHDFS
from tests.unit.output.test_local import TestOutputLOCAL


@pytest.mark.skipif(
    sys.version_info[0] == 2 and os.name == "nt",
    reason="Not supported for python 2 on Windows.",
)
class TestOutputHDFS(TestOutputLOCAL):
    def _get_cls(self):
        return OutputHDFS
