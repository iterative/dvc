from dvc.output.gdrive import GDriveOutput
from dvc.tree import GDriveTree
from tests.unit.output.test_local import TestLocalOutput


class TestGDriveOutput(TestLocalOutput):
    @property
    def _path(self):
        return "gdrive://path"

    @property
    def _tree(self):
        return GDriveTree(self.dvc, {"url": self._path})

    def _get_cls(self):
        return GDriveOutput
