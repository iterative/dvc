import pytest

from dvc.parsing import DataResolver, MatrixDefinition

MATRIX_DATA = {
    "os": ["win", "linux"],
    "pyv": [3.7, 3.8],
    "dict": [{"arg1": 1}, {"arg2": 2}],
    "list": [["out1", "out11"], ["out2", "out22"]],
}


@pytest.mark.parametrize(
    "matrix",
    [
        MATRIX_DATA,
        {
            "os": "${os}",
            "pyv": "${pyv}",
            "dict": "${dict}",
            "list": "${list}",
        },
    ],
)
def test_matrix_interpolated(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump(MATRIX_DATA)
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "cmd": "echo ${item.os} ${item.pyv} ${item.dict}"
        " -- ${item.list.0} ${item.list.1}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@win-3.7-dict0-list0": {"cmd": "echo win 3.7 --arg1 1 -- out1 out11"},
        "build@win-3.7-dict0-list1": {"cmd": "echo win 3.7 --arg1 1 -- out2 out22"},
        "build@win-3.7-dict1-list0": {"cmd": "echo win 3.7 --arg2 2 -- out1 out11"},
        "build@win-3.7-dict1-list1": {"cmd": "echo win 3.7 --arg2 2 -- out2 out22"},
        "build@win-3.8-dict0-list0": {"cmd": "echo win 3.8 --arg1 1 -- out1 out11"},
        "build@win-3.8-dict0-list1": {"cmd": "echo win 3.8 --arg1 1 -- out2 out22"},
        "build@win-3.8-dict1-list0": {"cmd": "echo win 3.8 --arg2 2 -- out1 out11"},
        "build@win-3.8-dict1-list1": {"cmd": "echo win 3.8 --arg2 2 -- out2 out22"},
        "build@linux-3.7-dict0-list0": {"cmd": "echo linux 3.7 --arg1 1 -- out1 out11"},
        "build@linux-3.7-dict0-list1": {"cmd": "echo linux 3.7 --arg1 1 -- out2 out22"},
        "build@linux-3.7-dict1-list0": {"cmd": "echo linux 3.7 --arg2 2 -- out1 out11"},
        "build@linux-3.7-dict1-list1": {"cmd": "echo linux 3.7 --arg2 2 -- out2 out22"},
        "build@linux-3.8-dict0-list0": {"cmd": "echo linux 3.8 --arg1 1 -- out1 out11"},
        "build@linux-3.8-dict0-list1": {"cmd": "echo linux 3.8 --arg1 1 -- out2 out22"},
        "build@linux-3.8-dict1-list0": {"cmd": "echo linux 3.8 --arg2 2 -- out1 out11"},
        "build@linux-3.8-dict1-list1": {"cmd": "echo linux 3.8 --arg2 2 -- out2 out22"},
    }


@pytest.mark.parametrize(
    "matrix",
    [
        MATRIX_DATA,
        {
            "os": "${os}",
            "pyv": "${pyv}",
            "dict": "${dict}",
            "list": "${list}",
        },
    ],
)
def test_matrix_key_present(tmp_dir, dvc, matrix):
    (tmp_dir / "params.yaml").dump(MATRIX_DATA)
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "cmd": "echo ${key}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@win-3.7-dict0-list0": {"cmd": "echo win-3.7-dict0-list0"},
        "build@win-3.7-dict0-list1": {"cmd": "echo win-3.7-dict0-list1"},
        "build@win-3.7-dict1-list0": {"cmd": "echo win-3.7-dict1-list0"},
        "build@win-3.7-dict1-list1": {"cmd": "echo win-3.7-dict1-list1"},
        "build@win-3.8-dict0-list0": {"cmd": "echo win-3.8-dict0-list0"},
        "build@win-3.8-dict0-list1": {"cmd": "echo win-3.8-dict0-list1"},
        "build@win-3.8-dict1-list0": {"cmd": "echo win-3.8-dict1-list0"},
        "build@win-3.8-dict1-list1": {"cmd": "echo win-3.8-dict1-list1"},
        "build@linux-3.7-dict0-list0": {"cmd": "echo linux-3.7-dict0-list0"},
        "build@linux-3.7-dict0-list1": {"cmd": "echo linux-3.7-dict0-list1"},
        "build@linux-3.7-dict1-list0": {"cmd": "echo linux-3.7-dict1-list0"},
        "build@linux-3.7-dict1-list1": {"cmd": "echo linux-3.7-dict1-list1"},
        "build@linux-3.8-dict0-list0": {"cmd": "echo linux-3.8-dict0-list0"},
        "build@linux-3.8-dict0-list1": {"cmd": "echo linux-3.8-dict0-list1"},
        "build@linux-3.8-dict1-list0": {"cmd": "echo linux-3.8-dict1-list0"},
        "build@linux-3.8-dict1-list1": {"cmd": "echo linux-3.8-dict1-list1"},
    }
