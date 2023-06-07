"""Testing happy paths for the foreach."""

import pytest

from dvc.parsing import DataResolver, ForeachDefinition
from dvc.parsing.context import Context


def test_with_simple_list_data(tmp_dir, dvc):
    """Testing a simple non-nested list as a foreach data"""
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})

    context = Context()
    data = {"foreach": ["foo", "bar", "baz"], "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("foo") == {"build@foo": {"cmd": "echo foo"}}
    assert definition.resolve_one("bar") == {"build@bar": {"cmd": "echo bar"}}
    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context
    assert not resolver.tracked_vars["build@foo"]
    assert not resolver.tracked_vars["build@bar"]


def test_with_dict_data(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    context = Context()

    foreach_data = {"model1": "foo", "model2": "bar"}
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${key} ${item}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("model1") == {
        "build@model1": {"cmd": "echo model1 foo"}
    }
    assert definition.resolve_one("model2") == {
        "build@model2": {"cmd": "echo model2 bar"}
    }

    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context
    assert not resolver.tracked_vars["build@model1"]
    assert not resolver.tracked_vars["build@model2"]


def test_with_dict_with_non_str_keys(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    context = Context()

    foreach_data = {2021: {"thresh": "foo"}, 2022: {"thresh": "bar"}}
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${key} ${item.thresh}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("2021") == {"build@2021": {"cmd": "echo 2021 foo"}}
    assert definition.resolve_one("2022") == {"build@2022": {"cmd": "echo 2022 bar"}}

    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context
    assert not resolver.tracked_vars["build@2021"]
    assert not resolver.tracked_vars["build@2022"]


def test_with_composite_list(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir.fs_path, {})

    context = Context()
    foreach_data = [{"thresh": "foo"}, {"thresh": "bar"}]
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, context, "build", data)

    assert definition.resolve_one("0") == {"build@0": {"cmd": "echo foo"}}
    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert not context

    assert definition.resolve_one("1") == {"build@1": {"cmd": "echo bar"}}
    assert not context
    assert not resolver.tracked_vars["build@0"]


def test_foreach_interpolated_simple_list(tmp_dir, dvc):
    foreach_data = ["foo", "bar", "baz"]
    vars_ = {"models": foreach_data}
    resolver = DataResolver(dvc, tmp_dir.fs_path, {"vars": [vars_]})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@foo": {"cmd": "echo foo"},
        "build@bar": {"cmd": "echo bar"},
        "build@baz": {"cmd": "echo baz"},
    }
    assert resolver.context == {"models": foreach_data}
    assert not any(item for item in resolver.tracked_vars.values())


@pytest.mark.parametrize("foreach_def", ["${item.thresh}", "${item[thresh]}"])
@pytest.mark.parametrize(
    "foreach_data, result",
    [
        (
            {"model1": {"thresh": "foo"}, "model2": {"thresh": "bar"}},
            {
                "build@model1": {"cmd": "echo foo"},
                "build@model2": {"cmd": "echo bar"},
            },
        ),
        (
            [{"thresh": "foo"}, {"thresh": "bar"}],
            {"build@0": {"cmd": "echo foo"}, "build@1": {"cmd": "echo bar"}},
        ),
    ],
)
def test_foreach_interpolate_with_composite_data(
    tmp_dir, dvc, foreach_def, foreach_data, result
):
    vars_ = [{"models": foreach_data}]
    resolver = DataResolver(dvc, tmp_dir.fs_path, {"vars": vars_})
    data = {"foreach": "${models}", "do": {"cmd": f"echo {foreach_def}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == result
    assert resolver.context == {"models": foreach_data}
    assert not any(item for item in resolver.tracked_vars.values())


def test_params_file_with_dict_tracked(tmp_dir, dvc):
    foreach_data = {"model1": {"thresh": "foo"}, "model2": {"thresh": "bar"}}
    params = {"models": foreach_data}
    (tmp_dir / "params.yaml").dump(params)

    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo foo"},
        "build@model2": {"cmd": "echo bar"},
    }
    # check that `foreach` item-key replacement didnot leave any leftovers.
    assert resolver.context == {"models": foreach_data}
    assert resolver.tracked_vars == {
        "build@model1": {"params.yaml": {"models.model1.thresh": "foo"}},
        "build@model2": {"params.yaml": {"models.model2.thresh": "bar"}},
    }


def test_params_file_tracked_for_composite_list(tmp_dir, dvc):
    foreach_data = [{"thresh": "foo"}, {"thresh": "bar"}]
    params = {"models": foreach_data}
    (tmp_dir / "params.yaml").dump(params)

    resolver = DataResolver(dvc, tmp_dir.fs_path, {})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@0": {"cmd": "echo foo"},
        "build@1": {"cmd": "echo bar"},
    }
    assert resolver.context == {"models": foreach_data}
    assert resolver.tracked_vars == {
        "build@0": {"params.yaml": {"models.0.thresh": "foo"}},
        "build@1": {"params.yaml": {"models.1.thresh": "bar"}},
    }


def test_foreach_data_from_nested_vars(tmp_dir, dvc):
    vars_ = {"models": {"lst": [{"thresh": 10}, {"thresh": 15}]}}
    resolver = DataResolver(dvc, tmp_dir.fs_path, {"vars": [vars_]})
    data = {"foreach": "${models.lst}", "do": {"cmd": "echo ${item.thresh}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@0": {"cmd": "echo 10"},
        "build@1": {"cmd": "echo 15"},
    }
    assert resolver.context == vars_
    assert not any(item for item in resolver.tracked_vars.values())


def test_foreach_partial_interpolations(tmp_dir, dvc):
    resolver = DataResolver(dvc, tmp_dir.fs_path, {"vars": [{"bar": "bar"}]})
    foreach_data = {"model1": "foo", "model2": "${bar}"}
    data = {"foreach": foreach_data, "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo foo"},
        "build@model2": {"cmd": "echo bar"},
    }
    assert resolver.context == {"bar": "bar"}
    assert not any(item for item in resolver.tracked_vars.values())


def test_mixed_vars_for_foreach_data(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump({"models": {"model1": "foo"}})
    (tmp_dir / "test_params.yaml").dump({"models": {"model2": "bar"}})

    resolver = DataResolver(dvc, tmp_dir.fs_path, {"vars": ["test_params.yaml"]})
    data = {"foreach": "${models}", "do": {"cmd": "echo ${item}"}}
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo foo"},
        "build@model2": {"cmd": "echo bar"},
    }
    assert resolver.context == {"models": {"model1": "foo", "model2": "bar"}}
    assert resolver.tracked_vars == {
        "build@model1": {"params.yaml": {"models.model1": "foo"}},
        "build@model2": {"test_params.yaml": {"models.model2": "bar"}},
    }


def test_mixed_vars_for_foreach_data_2(tmp_dir, dvc):
    (tmp_dir / "params.yaml").dump(
        {"models": {"model1": {"thresh": 10}, "model2": {"thresh": 15}}},
    )
    (tmp_dir / "test_params.yaml").dump(
        {"models": {"model1": {"epochs": 5}, "model2": {"epochs": 10}}},
    )

    resolver = DataResolver(dvc, tmp_dir.fs_path, {"vars": ["test_params.yaml"]})
    data = {
        "foreach": "${models}",
        "do": {"cmd": "echo ${item.thresh} ${item.epochs}"},
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        "build@model1": {"cmd": "echo 10 5"},
        "build@model2": {"cmd": "echo 15 10"},
    }
    assert resolver.context == {
        "models": {
            "model1": {"thresh": 10, "epochs": 5},
            "model2": {"thresh": 15, "epochs": 10},
        }
    }
    assert resolver.tracked_vars == {
        "build@model1": {
            "params.yaml": {"models.model1.thresh": 10},
            "test_params.yaml": {"models.model1.epochs": 5},
        },
        "build@model2": {
            "params.yaml": {"models.model2.thresh": 15},
            "test_params.yaml": {"models.model2.epochs": 10},
        },
    }


def test_foreach_with_interpolated_wdir(tmp_dir, dvc):
    resolver = DataResolver(dvc, (tmp_dir / "data").fs_path, {})
    foreach_data = ["foo", "bar"]
    data = {
        "foreach": foreach_data,
        "do": {"wdir": "${item}", "cmd": "echo hello"},
    }
    definition = ForeachDefinition(resolver, resolver.context, "build", data)

    assert definition.resolve_all() == {
        # note that the resolver generates `wdir` relative to file's wdir
        # so, this is just `foo`, not `data/foo`.
        # figuring out `wdir` is the responsibility of the `load_stage`/`Stage`
        "build@foo": {"wdir": "foo", "cmd": "echo hello"},
        "build@bar": {"wdir": "bar", "cmd": "echo hello"},
    }

    assert not resolver.context
    assert not any(item for item in resolver.tracked_vars.values())


def test_foreach_do_syntax_is_checked_once(tmp_dir, dvc, mocker):
    do_def = {"cmd": "python script.py --epochs ${item}"}
    data = {"foreach": [0, 1, 2, 3, 4], "do": do_def}
    definition = ForeachDefinition(
        DataResolver(dvc, tmp_dir.fs_path, {}), Context(), "build", data
    )
    mock = mocker.patch("dvc.parsing.check_syntax_errors", return_value=True)
    definition.resolve_all()

    mock.assert_called_once_with(do_def, "build", "dvc.yaml")


def test_foreach_data_is_only_resolved_once(tmp_dir, dvc, mocker):
    context = Context(models=["foo", "bar", "baz"])
    data = {"foreach": "${models}", "do": {}}
    definition = ForeachDefinition(
        DataResolver(dvc, tmp_dir.fs_path, {}), context, "build", data
    )
    mock = mocker.spy(definition, "_resolve_foreach_data")

    definition.resolve_all()

    mock.assert_called_once_with()
