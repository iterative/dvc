import pytest

from dvc.parsing import DataResolver, MatrixDefinition, ResolveError

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


def test_matrix_custom_name(tmp_dir, dvc):
    matrix = {
        "dataset": [{"key": "dataset_a"}],
        "model": [{"key": "model_alpha"}],
    }
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "name": "${item.model.key}_${item.dataset.key}",
        "cmd": "echo ${item.model.key} ${item.dataset.key}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "inference", data)

    assert definition.get_generated_names() == ["inference@model_alpha_dataset_a"]
    assert definition.has_member("model_alpha_dataset_a")
    assert definition.resolve_one("model_alpha_dataset_a") == {
        "inference@model_alpha_dataset_a": {"cmd": "echo model_alpha dataset_a"}
    }


def test_matrix_custom_name_duplicate_error(tmp_dir, dvc):
    matrix = {"model": [{"key": "same"}, {"key": "same"}]}
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "name": "${item.model.key}",
        "cmd": "echo ${item.model.key}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "train", data)

    with pytest.raises(ResolveError, match="already defined"):
        definition.get_generated_names()


def test_matrix_custom_name_invalid_suffix(tmp_dir, dvc):
    matrix = {"model": [{"key": "same"}]}
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {
        "matrix": matrix,
        "name": "bad@name",
        "cmd": "echo ${item.model.key}",
    }
    definition = MatrixDefinition(resolver, resolver.context, "train", data)

    with pytest.raises(ResolveError, match="cannot contain"):
        definition.get_generated_names()
