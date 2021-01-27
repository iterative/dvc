import os

import pytest
from voluptuous import Schema as _Schema

from dvc import output
from dvc.schema import SINGLE_PIPELINE_STAGE_SCHEMA
from dvc.stage.serialize import to_pipeline_file as _to_pipeline_file

Schema = _Schema(SINGLE_PIPELINE_STAGE_SCHEMA)


def to_pipeline_file(stage):
    """Validate schema on each serialization."""
    e = _to_pipeline_file(stage)
    assert len(Schema(e)) == 1
    return e


def test_cmd(dvc, make_stage):
    stage = make_stage()
    entry = to_pipeline_file(stage)
    assert entry == {"something": {"cmd": "command"}}


def test_wdir(dvc, make_stage):
    stage = make_stage()
    assert stage.PARAM_WDIR not in to_pipeline_file(stage)["something"]

    stage.wdir = os.curdir
    assert stage.PARAM_WDIR not in to_pipeline_file(stage)["something"]

    stage.wdir = "some-dir"
    assert to_pipeline_file(stage)["something"][stage.PARAM_WDIR] == "some-dir"


def test_deps_sorted(dvc, make_stage):
    stage = make_stage(deps=["a", "quick", "lazy", "fox"])
    assert to_pipeline_file(stage)["something"][stage.PARAM_DEPS] == [
        "a",
        "fox",
        "lazy",
        "quick",
    ]


def test_outs_sorted(dvc, make_stage):
    stage = make_stage(outs=["too", "many", "outs"], deps=["foo"])
    assert to_pipeline_file(stage)["something"][stage.PARAM_OUTS] == [
        "many",
        "outs",
        "too",
    ]


def test_params_sorted(dvc, make_stage):
    stage = make_stage(
        outs=["bar"],
        deps=["foo"],
        params=[
            "lorem",
            "ipsum",
            {"custom.yaml": ["wxyz", "pqrs", "baz"]},
            {"params.yaml": ["barr"]},
        ],
    )
    assert to_pipeline_file(stage)["something"][stage.PARAM_PARAMS] == [
        "barr",
        "ipsum",
        "lorem",
        {"custom.yaml": ["baz", "pqrs", "wxyz"]},
    ]


def test_params_file_sorted(dvc, make_stage):
    stage = make_stage(
        outs=["bar"],
        deps=["foo"],
        params=[
            "lorem",
            "ipsum",
            {"custom.yaml": ["wxyz", "pqrs", "baz"]},
            {"a-file-of-params.yaml": ["barr"]},
        ],
    )
    assert to_pipeline_file(stage)["something"][stage.PARAM_PARAMS] == [
        "ipsum",
        "lorem",
        {"a-file-of-params.yaml": ["barr"]},
        {"custom.yaml": ["baz", "pqrs", "wxyz"]},
    ]


@pytest.mark.parametrize(
    "typ, extra",
    [("plots", {"plot": True}), ("metrics", {"metric": True}), ("outs", {})],
)
def test_outs_and_outs_flags_are_sorted(dvc, typ, make_stage, extra):
    stage = make_stage(deps=["input"])
    stage.outs += output.loads_from(stage, ["barr"], use_cache=False, **extra)
    stage.outs += output.loads_from(
        stage, ["foobar"], use_cache=False, persist=True, **extra
    )
    stage.outs += output.loads_from(stage, ["foo"], persist=True, **extra)
    stage.outs += output.loads_from(stage, ["bar"], **extra)

    serialized_outs = to_pipeline_file(stage)["something"][typ]
    assert serialized_outs == [
        "bar",
        {"barr": {"cache": False}},
        {"foo": {"persist": True}},
        {"foobar": {"cache": False, "persist": True}},
    ]
    assert list(serialized_outs[3]["foobar"].keys()) == ["cache", "persist"]


def test_plot_props(dvc, make_stage):
    props = {"x": "1"}
    stage = make_stage(plots=["plot_file"])
    stage.outs[0].plot = props

    assert to_pipeline_file(stage)["something"][stage.PARAM_PLOTS] == [
        {"plot_file": props}
    ]


def test_frozen(dvc, make_stage):
    stage = make_stage(outs=["output"], deps=["input"])
    assert stage.PARAM_FROZEN not in to_pipeline_file(stage)["something"]

    stage = make_stage(frozen=True)
    assert to_pipeline_file(stage)["something"][stage.PARAM_FROZEN] is True


def test_always_changed(dvc, make_stage):
    stage = make_stage(outs=["output"], deps=["input"])
    assert (
        stage.PARAM_ALWAYS_CHANGED not in to_pipeline_file(stage)["something"]
    )

    stage = make_stage(always_changed=True)
    assert (
        to_pipeline_file(stage)["something"][stage.PARAM_ALWAYS_CHANGED]
        is True
    )


def test_order(dvc, make_stage):
    stage = make_stage(
        outs=["output"], deps=["input"], always_changed=True, frozen=True,
    )

    # `create_stage` checks for existence of `wdir`
    stage.wdir = "some-dir"
    assert list(to_pipeline_file(stage)["something"].keys()) == [
        "cmd",
        "wdir",
        "deps",
        "outs",
        "frozen",
        "always_changed",
    ]


@pytest.mark.parametrize(
    "typ", ["outs", "metrics", "plots", "params", "deps", None]
)
def test_order_deps_outs(dvc, make_stage, typ):
    all_types = ["deps", "params", "outs", "metrics", "plots"]
    all_types = [item for item in all_types if item != typ]
    extra = {key: [f"foo-{i}"] for i, key in enumerate(all_types)}

    stage = make_stage(**extra)
    assert typ not in to_pipeline_file(stage)["something"]
    assert (
        list(to_pipeline_file(stage)["something"].keys())
        == ["cmd"] + all_types
    )
