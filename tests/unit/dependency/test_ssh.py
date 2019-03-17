from dvc.dependency.ssh import DependencySSH

from tests.unit.dependency.test_local import TestDependencyLOCAL


class TestDependencySSH(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencySSH
