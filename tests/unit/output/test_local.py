import mock

from dvc.stage import Stage
from dvc.output import OutputLOCAL

from tests.basic_env import TestDvc


class TestOutputLOCAL(TestDvc):
    def _get_cls(self):
        return OutputLOCAL

    def _get_output(self):
        stage = Stage(self.dvc)
        return self._get_cls()(stage, "path", cache=False)

    def test_save_missing(self):
        o = self._get_output()
        with mock.patch.object(o.remote, "exists", return_value=False):
            with self.assertRaises(o.DoesNotExistError):
                o.save()
