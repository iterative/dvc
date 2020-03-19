import mock

from dvc.dependency import DependencyPARAMS
from dvc.stage import Stage
from tests.basic_env import TestDvc


class TestDependencyPARAM(TestDvc):
    def test_from_list(self):
        stage = Stage(self.dvc)
        deps = DependencyPARAMS.from_list(
            stage, ["foo", "bar,baz", "a_file:qux"]
        )
        assert len(deps) == 2
        assert deps[0].def_path == "a_file"
        assert deps[0].param_names == ["qux"]
        assert deps[1].def_path == DependencyPARAMS.DEFAULT_PARAMS_FILE
        assert deps[1].param_names == ["bar", "baz", "foo"]
