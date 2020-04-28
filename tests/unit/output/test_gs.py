from dvc.output.gs import GSOutput
from tests.unit.output.test_local import TestLocalOutput


class TestGSOutput(TestLocalOutput):
    def _get_cls(self):
        return GSOutput
