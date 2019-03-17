import mock

from dvc.stage import Stage
from dvc.dependency import DependencyLOCAL

from tests.basic_env import TestDvc


class TestDependencyLOCAL(TestDvc):
    def _get_cls(self):
        return DependencyLOCAL

    def _get_dependency(self):
        stage = Stage(self.dvc)
        return self._get_cls()(stage, "path")

    def test_save_missing(self):
        d = self._get_dependency()
        with mock.patch.object(d.remote, "exists", return_value=False):
            with self.assertRaises(d.DoesNotExistError):
                d.save()
