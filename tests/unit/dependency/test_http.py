from dvc.dependency.http import DependencyHTTP

from tests.unit.dependency.test_local import TestDependencyLOCAL


class TestDependencyHTTP(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencyHTTP
