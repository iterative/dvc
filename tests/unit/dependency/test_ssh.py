from dvc.dependency.ssh import SSHDependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestSSHDependency(TestLocalDependency):
    def _get_cls(self):
        return SSHDependency
