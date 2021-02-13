import mock

from dvc.dependency import LocalDependency
from dvc.stage import Stage
from tests.basic_env import TestDvc


class TestLocalDependency(TestDvc):
    def _get_cls(self):
        return LocalDependency

    def _get_dependency(self):
        stage = Stage(self.dvc)
        return self._get_cls()(stage, "path")

    def test_save_missing(self):
        d = self._get_dependency()
        with mock.patch.object(d.fs, "exists", return_value=False):
            with self.assertRaises(d.DoesNotExistError):
                d.save()
