from dvc.dependency.gs import DependencyGS

from tests.unit.dependency.test_local import TestDependencyLOCAL


class TestDependencyGS(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencyGS
