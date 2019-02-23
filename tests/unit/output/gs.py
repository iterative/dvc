from dvc.output.gs import OutputGS

from tests.unit.output.local import TestOutputLOCAL


class TestOutputGS(TestOutputLOCAL):
    def _get_cls(self):
        return OutputGS
