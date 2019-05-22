from dvc.output.ssh import OutputSSH

from tests.unit.output.test_local import TestOutputLOCAL


class TestOutputSSH(TestOutputLOCAL):
    def _get_cls(self):
        return OutputSSH
