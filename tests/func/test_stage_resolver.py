import os
from copy import deepcopy

import pytest

from dvc.dependency import _merge_params
from dvc.parsing import DEFAULT_PARAMS_FILE, DataResolver
from dvc.path_info import PathInfo
from dvc.utils.serialize import dump_json, dump_yaml

TEMPLATED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py ${dict.foo} --out ${dict.bar}",
            "outs": ["${dict.bar}"],
            "deps": ["${dict.foo}"],
            "frozen": "${freeze}",
        },
        "stage2": {"cmd": "echo ${dict.foo} ${dict.bar}"},
    }
}

CONTEXT_DATA = {
    "dict": {"foo": "foo", "bar": "bar"},
    "list": ["param1", "param2"],
    "freeze": True,
}

RESOLVED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py foo --out bar",
            "outs": ["bar"],
            "deps": ["foo"],
            "params": [{"params.yaml": ["dict.foo", "dict.bar", "freeze"]}],
            "frozen": True,
        },
        "stage2": {
            "cmd": "echo foo bar",
            "params": [{"params.yaml": ["dict.foo", "dict.bar"]}],
        },
    }
}


def assert_stage_equal(d1, d2):
    """Keeps the params section in order, and then checks for equality."""
    for d in [d1, d2]:
        for _, stage_d in d.get("stages", {}).items():
            params = _merge_params(stage_d.get("params", []))
            for k in params:
                params[k] = sorted(params[k])
            if params:
                stage_d["params"] = params
    assert d1 == d2


def test_simple(tmp_dir, dvc):
    dump_yaml(tmp_dir / DEFAULT_PARAMS_FILE, CONTEXT_DATA)
    resolver = DataResolver(
        dvc, PathInfo(str(tmp_dir)), deepcopy(TEMPLATED_DVC_YAML_DATA)
    )
    assert_stage_equal(resolver.resolve(), deepcopy(RESOLVED_DVC_YAML_DATA))


def test_vars(tmp_dir, dvc):
    d = deepcopy(TEMPLATED_DVC_YAML_DATA)
    d["vars"] = CONTEXT_DATA
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    resolved_data = deepcopy(RESOLVED_DVC_YAML_DATA)

    # `vars` section is not auto-tracked
    del resolved_data["stages"]["stage1"]["params"]
    del resolved_data["stages"]["stage2"]["params"]
    assert_stage_equal(resolver.resolve(), resolved_data)


def test_no_params_yaml_and_vars(tmp_dir, dvc):
    resolver = DataResolver(
        dvc, PathInfo(str(tmp_dir)), deepcopy(TEMPLATED_DVC_YAML_DATA)
    )
    with pytest.raises(ValueError):
        resolver.resolve()


def test_use(tmp_dir, dvc):
    """
    Test that different file can be loaded using `use`
    instead of default params.yaml.
    """
    dump_yaml(tmp_dir / "params2.yaml", CONTEXT_DATA)
    d = deepcopy(TEMPLATED_DVC_YAML_DATA)
    d["use"] = "params2.yaml"
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)

    resolved_data = deepcopy(RESOLVED_DVC_YAML_DATA)
    for _, stage_d in resolved_data["stages"].items():
        params = stage_d["params"][0]
        params["params2.yaml"] = params.pop(DEFAULT_PARAMS_FILE)

    assert_stage_equal(resolver.resolve(), resolved_data)


def test_vars_and_params_import(tmp_dir, dvc):
    """
    Test that vars and params are both merged together for interpolation,
    whilst tracking the "used" variables from params.
    """
    d = {
        "use": DEFAULT_PARAMS_FILE,
        "vars": {"dict": {"foo": "foobar"}},
        "stages": {"stage1": {"cmd": "echo ${dict.foo} ${dict.bar}"}},
    }
    dump_yaml(tmp_dir / DEFAULT_PARAMS_FILE, {"dict": {"bar": "bar"}})
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)

    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "stage1": {"cmd": "echo foobar bar", "params": ["dict.bar"]}
            }
        },
    )


def test_with_params_section(tmp_dir, dvc):
    """Test that params section is also loaded for interpolation"""
    d = {
        "use": "params.yaml",
        "vars": {"dict": {"foo": "foo"}},
        "stages": {
            "stage1": {
                "cmd": "echo ${dict.foo} ${dict.bar} ${dict.foobar}",
                "params": [{"params.json": ["value1"]}],
            },
        },
    }
    dump_yaml(tmp_dir / DEFAULT_PARAMS_FILE, {"dict": {"bar": "bar"}})
    dump_json(tmp_dir / "params.json", {"dict": {"foobar": "foobar"}})
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "stage1": {
                    "cmd": "echo foo bar foobar",
                    "params": [
                        "dict.bar",
                        {"params.json": ["dict.foobar", "value1"]},
                    ],
                }
            }
        },
    )


def test_stage_with_wdir(tmp_dir, dvc):
    """
    Test that params file from wdir are also loaded
    """
    d = {
        "stages": {
            "stage1": {
                "cmd": "echo ${dict.foo} ${dict.bar}",
                "params": ["value1"],
                "wdir": "data",
            },
        },
    }

    data_dir = tmp_dir / "data"
    data_dir.mkdir()
    dump_yaml(tmp_dir / DEFAULT_PARAMS_FILE, {"dict": {"bar": "bar"}})
    dump_json(data_dir / DEFAULT_PARAMS_FILE, {"dict": {"foo": "foo"}})
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)

    root_params_file = os.path.relpath(tmp_dir / "params.yaml", data_dir)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "stage1": {
                    "cmd": "echo foo bar",
                    "wdir": "data",
                    "params": [
                        "dict.foo",
                        "value1",
                        {root_params_file: ["dict.bar"]},
                    ],
                }
            }
        },
    )


def test_with_templated_wdir(tmp_dir, dvc):
    """
    Test that params from the resolved wdir are still loaded
    and is used in the interpolation.
    """
    d = {
        "stages": {
            "stage1": {
                "cmd": "echo ${dict.foo} ${dict.bar}",
                "params": ["value1"],
                "wdir": "${dict.ws}",
            },
        },
    }
    dump_yaml(
        tmp_dir / DEFAULT_PARAMS_FILE, {"dict": {"bar": "bar", "ws": "data"}}
    )
    data_dir = tmp_dir / "data"
    data_dir.mkdir()
    dump_json(data_dir / DEFAULT_PARAMS_FILE, {"dict": {"foo": "foo"}})
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)

    root_params_file = os.path.relpath(tmp_dir / "params.yaml", data_dir)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "stage1": {
                    "cmd": "echo foo bar",
                    "wdir": "data",
                    "params": [
                        "dict.foo",
                        "value1",
                        {root_params_file: ["dict.bar", "dict.ws"]},
                    ],
                }
            }
        },
    )
