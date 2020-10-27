from dvc.parsing import DataResolver
from dvc.parsing.context import Context

TEMPLATED_DVC_YAML_DATA = {
    "stages": {
        "stage1": {
            "cmd": "python script.py ${dict.foo} --out ${dict.bar}",
            "outs": ["${dict.bar}"],
            "deps": ["${dict.foo}"],
            "params": ["${list.0}", "${list.1}"],
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
            "params": ["param1", "param2"],
            "frozen": True,
        },
        "stage2": {"cmd": "echo foo bar"},
    }
}


def test_resolver(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir, TEMPLATED_DVC_YAML_DATA)
    resolver.global_ctx = Context(CONTEXT_DATA)
    assert resolver.resolve() == RESOLVED_DVC_YAML_DATA
