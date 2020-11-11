import mock

from dvc.dependency import LocalDependency
from dvc.stage import Stage
from tests.basic_env import TestDvc


class TestLocalDependency(TestDvc):
    @property
    def _path(self):
        return "path"

    def _get_cls(self):
        return LocalDependency

    def _get_dependency(self):
        stage = Stage(self.dvc)
        return self._get_cls()(stage, self._path)

    def test_save_missing(self):
        d = self._get_dependency()
        with mock.patch.object(d.tree, "exists", return_value=False):
            with self.assertRaises(d.DoesNotExistError):
                d.save()
