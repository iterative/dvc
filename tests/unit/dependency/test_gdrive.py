from dvc.dependency.gdrive import GDriveDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestGDriveDependency(TestLocalDependency):
    def _get_cls(self):
        return GDriveDependency
