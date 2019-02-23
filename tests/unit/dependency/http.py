from dvc.dependency.http import DependencyHTTP

from tests.unit.dependency.local import TestDependencyLOCAL


class TestDependencyHTTP(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencyHTTP
