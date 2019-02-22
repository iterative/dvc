from dvc.dependency.gs import DependencyGS

from tests.unit.dependency.local import TestDependencyLOCAL


class TestDependencyGS(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencyGS
