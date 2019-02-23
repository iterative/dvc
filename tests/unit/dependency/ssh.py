from dvc.dependency.ssh import DependencySSH

from tests.unit.dependency.local import TestDependencyLOCAL


class TestDependencySSH(TestDependencyLOCAL):
    def _get_cls(self):
        return DependencySSH
