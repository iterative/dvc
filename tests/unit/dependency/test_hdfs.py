import unittest

from dvc.dependency.hdfs import DependencyHDFS
from tests.remotes import HDFS
from tests.unit.dependency.test_local import TestDependencyLOCAL


@unittest.skipIf(not HDFS.should_test(), "skip if there's no HDFS")
class TestDependencyHDFS(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencyHDFS
