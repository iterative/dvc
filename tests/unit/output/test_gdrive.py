from dvc.output.gdrive import GDriveOutput
from tests.unit.output.test_local import TestLocalOutput


class TestGDriveOutput(TestLocalOutput):
    def _get_cls(self):
        return GDriveOutput
