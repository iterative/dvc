from dvc.dependency.gdrive import GDriveDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestGDriveDependency(TestLocalDependency):
    @property
    def _path(self):
        return "gdrive://path"

    def _get_cls(self):
        return GDriveDependency
