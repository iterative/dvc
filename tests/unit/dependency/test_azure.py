from dvc.dependency.azure import AzureDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestAzureDependency(TestLocalDependency):
    def _get_cls(self):
        return AzureDependency
