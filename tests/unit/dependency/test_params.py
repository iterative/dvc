import pytest

from dvc.dependency import ParamsDependency, loadd_from, loads_params
from dvc.dependency.param import BadParamFileError, MissingParamsError
from dvc.stage import Stage
from dvc.utils.serialize import dump_toml, dump_yaml, load_yaml

PARAMS = {
    "foo": 1,
    "bar": 53.135,
    "baz": "str",
    "qux": None,
}
DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE


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
        "path": DEFAULT_PARAMS_FILE,
        "params": PARAMS,
    }


def test_dumpd_without_info(dvc):
    dep = ParamsDependency(Stage(dvc), None, list(PARAMS.keys()))
    assert dep.dumpd() == {
        "path": DEFAULT_PARAMS_FILE,
        "params": list(PARAMS.keys()),
    }


def test_read_params_nonexistent_file(dvc):
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    assert dep.read_params() == {}


def test_read_params_unsupported_format(tmp_dir, dvc):
    tmp_dir.gen(DEFAULT_PARAMS_FILE, b"\0\1\2\3\4\5\6\7")
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    with pytest.raises(BadParamFileError):
        dep.read_params()


def test_read_params_nested(tmp_dir, dvc):
    dump_yaml(
        DEFAULT_PARAMS_FILE, {"some": {"path": {"foo": ["val1", "val2"]}}}
    )
    dep = ParamsDependency(Stage(dvc), None, ["some.path.foo"])
    assert dep.read_params() == {"some.path.foo": ["val1", "val2"]}


def test_read_params_default_loader(tmp_dir, dvc):
    parameters_file = "parameters.foo"
    dump_yaml(parameters_file, {"some": {"path": {"foo": ["val1", "val2"]}}})
    dep = ParamsDependency(Stage(dvc), parameters_file, ["some.path.foo"])
    assert dep.read_params() == {"some.path.foo": ["val1", "val2"]}


def test_read_params_wrong_suffix(tmp_dir, dvc):
    parameters_file = "parameters.toml"
    dump_yaml(parameters_file, {"some": {"path": {"foo": ["val1", "val2"]}}})
    dep = ParamsDependency(Stage(dvc), parameters_file, ["some.path.foo"])
    with pytest.raises(BadParamFileError):
        dep.read_params()


def test_read_params_toml(tmp_dir, dvc):
    parameters_file = "parameters.toml"
    dump_toml(parameters_file, {"some": {"path": {"foo": ["val1", "val2"]}}})
    dep = ParamsDependency(Stage(dvc), parameters_file, ["some.path.foo"])
    assert dep.read_params() == {"some.path.foo": ["val1", "val2"]}


def test_save_info_missing_config(dvc):
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    with pytest.raises(MissingParamsError):
        dep.save_info()


def test_save_info_missing_param(tmp_dir, dvc):
    tmp_dir.gen(DEFAULT_PARAMS_FILE, "bar: baz")
    dep = ParamsDependency(Stage(dvc), None, ["foo"])
    with pytest.raises(MissingParamsError):
        dep.save_info()


@pytest.mark.regression_4184
@pytest.mark.parametrize("param_value", ["", "false", "[]", "{}", "null"])
def test_params_with_false_values(tmp_dir, dvc, param_value):
    """These falsy params values should not ignored by `status` on loading."""
    key = "param"
    dep = ParamsDependency(Stage(dvc), DEFAULT_PARAMS_FILE, [key])
    (tmp_dir / DEFAULT_PARAMS_FILE).write_text(f"{key}: {param_value}")

    dep.fill_values(load_yaml(DEFAULT_PARAMS_FILE))

    with dvc.state:
        assert dep.status() == {}
