from dvc.dependency.gs import GSDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestGSDependency(TestLocalDependency):
    def _get_cls(self):
        return GSDependency
