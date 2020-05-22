from dvc.output.azure import AzureOutput
from tests.unit.output.test_local import TestLocalOutput


class TestAzureOutput(TestLocalOutput):
    def _get_cls(self):
        return AzureOutput
