import unittest

from dvc.output.hdfs import OutputHDFS
from tests.remotes import HDFS
from tests.unit.output.test_local import TestOutputLOCAL


@unittest.skipIf(not HDFS.should_test(), "skip if there's no HDFS")
class TestOutputHDFS(TestOutputLOCAL):
    def _get_cls(self):
        return OutputHDFS
