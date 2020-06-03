import pytest
import yaml

from dvc.dependency import ParamsDependency, loadd_from, loads_params
from dvc.dependency.param import BadParamFileError, MissingParamsError
from dvc.stage import Stage

PARAMS = {
    "foo": 1,
    "bar": 53.135,
    "baz": "str",
    "qux": None,
}


def test_loads_params(dvc):
    stage = Stage(dvc)
    deps = loads_params(
        stage,
        [
            "foo",
            "bar",
            {"a_file": ["baz", "bat"]},
            {"b_file": ["cat"]},
            {},
            {"a_file": ["foobar"]},
        ],
    )
    assert len(deps) == 3

    assert isinstance(deps[0], ParamsDependency)
    assert deps[0].def_path == ParamsDependency.DEFAULT_PARAMS_FILE
    assert deps[0].params == ["foo", "bar"]
    assert deps[0].info == {}

    assert isinstance(deps[1], ParamsDependency)
    assert deps[1].def_path == "a_file"
    assert deps[1].params == ["baz", "bat", "foobar"]
    assert deps[1].info == {}

    assert isinstance(deps[2], ParamsDependency)
    assert deps[2].def_path == "b_file"
    assert deps[2].params == ["cat"]
    assert deps[2].info == {}


@pytest.mark.parametrize("params", [[3], [{"b_file": "cat"}]])
def test_params_error(dvc, params):
    with pytest.raises(ValueError):
        loads_params(Stage(dvc), params)


def test_loadd_from(dvc):
    stage = Stage(dvc)
    deps = loadd_from(stage, [{"params": PARAMS}])
    assert len(deps) == 1
    assert isinstance(deps[0], ParamsDependency)
    assert deps[0].def_path == ParamsDependency.DEFAULT_PARAMS_FILE
    assert deps[0].params == list(PARAMS.keys())
    assert deps[0].info == PARAMS


def test_dumpd_with_info(dvc):
    dep = ParamsDependency(Stage(dvc), None, PARAMS)
    assert dep.dumpd() == {
        "path": "params.yaml",
        "params": PARAMS,
    }


def test_dumpd_without_info(dvc):
    dep = ParamsDependency(Stage(dvc), None, list(PARAMS.keys()))
    assert dep.dumpd() == {
        "path": "params.yaml",
        "params": list(PARAMS.keys()),
    }


def test_read_params_nonexistent_file(dvc):
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    assert dep.read_params() == {}


def test_read_params_unsupported_format(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", b"\0\1\2\3\4\5\6\7")
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    with pytest.raises(BadParamFileError):
        dep.read_params()


def test_read_params_nested(tmp_dir, dvc):
    tmp_dir.gen(
        "params.yaml", yaml.dump({"some": {"path": {"foo": ["val1", "val2"]}}})
    )
    dep = ParamsDependency(Stage(dvc), None, ["some.path.foo"])
    assert dep.read_params() == {"some.path.foo": ["val1", "val2"]}


def test_save_info_missing_config(dvc):
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    with pytest.raises(MissingParamsError):
        dep.save_info()


def test_save_info_missing_param(tmp_dir, dvc):
    tmp_dir.gen("params.yaml", "bar: baz")
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    with pytest.raises(MissingParamsError):
        dep.save_info()
