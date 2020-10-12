from dvc.output.ssh import SSHOutput
from tests.unit.output.test_local import TestLocalOutput


class TestSSHOutput(TestLocalOutput):
    def _get_cls(self):
        return SSHOutput
