import os
from copy import deepcopy
from math import pi

import pytest

from dvc.dependency import _merge_params
from dvc.parsing import DEFAULT_PARAMS_FILE, DataResolver
from dvc.parsing.context import Node
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


def recurse_not_a_node(d):
    assert not isinstance(d, Node)
    if isinstance(d, (list, dict)):
        iterable = d if isinstance(d, list) else d.values()
        for item in iterable:
            assert recurse_not_a_node(item)
    return True


def assert_stage_equal(d1, d2):
    """Keeps the params section in order, and then checks for equality."""
    for d in [d1, d2]:
        assert recurse_not_a_node(d)
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


def test_simple_foreach_loop(tmp_dir, dvc):
    iterable = ["foo", "bar", "baz"]
    d = {
        "stages": {
            "build": {
                "foreach": iterable,
                "in": {"cmd": "python script.py ${item}"},
            }
        }
    }

    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                f"build-{item}": {"cmd": f"python script.py {item}"}
                for item in iterable
            }
        },
    )


def test_foreach_loop_dict(tmp_dir, dvc):
    iterable = {"models": {"us": {"thresh": 10}, "gb": {"thresh": 15}}}
    d = {
        "stages": {
            "build": {
                "foreach": iterable["models"],
                "in": {"cmd": "python script.py ${item.thresh}"},
            }
        }
    }

    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                f"build-{key}": {"cmd": f"python script.py {item['thresh']}"}
                for key, item in iterable["models"].items()
            }
        },
    )


def test_foreach_loop_templatized(tmp_dir, dvc):
    params = {"models": {"us": {"thresh": 10}}}
    vars_ = {"models": {"gb": {"thresh": 15}}}
    dump_yaml(tmp_dir / DEFAULT_PARAMS_FILE, params)
    d = {
        "vars": vars_,
        "stages": {
            "build": {
                "foreach": "${models}",
                "in": {"cmd": "python script.py --thresh ${item.thresh}"},
            }
        },
    }

    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "build-gb": {"cmd": "python script.py --thresh 15"},
                "build-us": {
                    "cmd": "python script.py --thresh 10",
                    "params": ["models.us.thresh"],
                },
            }
        },
    )


@pytest.mark.parametrize(
    "value", ["value", "To set or not to set", 3, pi, True, False, None]
)
def test_set(tmp_dir, dvc, value):
    d = {
        "stages": {
            "build": {
                "set": {"item": value},
                "cmd": "python script.py --thresh ${item}",
                "always_changed": "${item}",
            }
        }
    }
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "build": {
                    "cmd": f"python script.py --thresh {value}",
                    "always_changed": value,
                }
            }
        },
    )


@pytest.mark.parametrize(
    "coll", [["foo", "bar", "baz"], {"foo": "foo", "bar": "bar"}]
)
def test_coll(tmp_dir, dvc, coll):
    d = {
        "stages": {
            "build": {
                "set": {"item": coll, "thresh": 10},
                "cmd": "python script.py --thresh ${thresh}",
                "outs": "${item}",
            }
        }
    }
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "build": {"cmd": "python script.py --thresh 10", "outs": coll}
            }
        },
    )


def test_set_with_foreach(tmp_dir, dvc):
    items = ["foo", "bar", "baz"]
    d = {
        "stages": {
            "build": {
                "set": {"items": items},
                "foreach": "${items}",
                "in": {"cmd": "command --value ${item}"},
            }
        }
    }
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                f"build-{item}": {"cmd": f"command --value {item}"}
                for item in items
            }
        },
    )


def test_set_with_foreach_and_on_stage_definition(tmp_dir, dvc):
    iterable = {"models": {"us": {"thresh": 10}, "gb": {"thresh": 15}}}
    dump_json(tmp_dir / "params.json", iterable)

    d = {
        "use": "params.json",
        "stages": {
            "build": {
                "set": {"data": "${models}"},
                "foreach": "${data}",
                "in": {
                    "set": {"thresh": "${item.thresh}"},
                    "cmd": "command --value ${thresh}",
                },
            }
        },
    }
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "build-us": {
                    "cmd": "command --value 10",
                    "params": [{"params.json": ["models.us.thresh"]}],
                },
                "build-gb": {
                    "cmd": "command --value 15",
                    "params": [{"params.json": ["models.gb.thresh"]}],
                },
            }
        },
    )


def test_resolve_local_tries_to_load_globally_used_files(tmp_dir, dvc):
    iterable = {"bar": "bar", "foo": "foo"}
    dump_json(tmp_dir / "params.json", iterable)

    d = {
        "use": "params.json",
        "stages": {
            "build": {
                "cmd": "command --value ${bar}",
                "params": [{"params.json": ["foo"]}],
            },
        },
    }
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "build": {
                    "cmd": "command --value bar",
                    "params": [{"params.json": ["bar", "foo"]}],
                },
            }
        },
    )


def test_resolve_local_tries_to_load_globally_used_params_yaml(tmp_dir, dvc):
    iterable = {"bar": "bar", "foo": "foo"}
    dump_yaml(tmp_dir / "params.yaml", iterable)

    d = {
        "stages": {
            "build": {
                "cmd": "command --value ${bar}",
                "params": [{"params.yaml": ["foo"]}],
            },
        },
    }
    resolver = DataResolver(dvc, PathInfo(str(tmp_dir)), d)
    assert_stage_equal(
        resolver.resolve(),
        {
            "stages": {
                "build": {
                    "cmd": "command --value bar",
                    "params": [{"params.yaml": ["bar", "foo"]}],
                },
            }
        },
    )
