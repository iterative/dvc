from dvc.dependency.hdfs import HDFSDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestHDFSDependency(TestLocalDependency):
    def _get_cls(self):
        return HDFSDependency
