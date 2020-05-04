import pytest
import os

import yaml

from dvc.stage.exceptions import InvalidStageName, DuplicateStageName


def test_run_with_name(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage
    from dvc.dvcfile import PIPELINE_FILE, PIPELINE_LOCK

    tmp_dir.dvc_gen("foo", "foo")
    assert not os.path.exists(PIPELINE_FILE)
    stage = run_copy("foo", "bar", name="copy-foo-to-bar")
    assert isinstance(stage, PipelineStage)
    assert stage.name == "copy-foo-to-bar"
    assert os.path.exists(PIPELINE_FILE)
    assert os.path.exists(PIPELINE_LOCK)


def test_run_with_multistage_and_single_stage(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage, Stage

    tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "foo1", single_stage=True)
    stage2 = run_copy("foo1", "foo2", name="copy-foo1-foo2")
    stage3 = run_copy("foo2", "foo3", single_stage=True)

    assert isinstance(stage2, PipelineStage)
    assert isinstance(stage1, Stage)
    assert isinstance(stage3, Stage)
    assert stage2.name == "copy-foo1-foo2"


def test_run_multi_stage_repeat(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage
    from dvc.dvcfile import Dvcfile, PIPELINE_FILE

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "foo1", name="copy-foo-foo1")
    run_copy("foo1", "foo2", name="copy-foo1-foo2")
    run_copy("foo2", "foo3", single_stage=True)

    stages = list(Dvcfile(dvc, PIPELINE_FILE).stages.values())
    assert len(stages) == 2
    assert all(isinstance(stage, PipelineStage) for stage in stages)
    assert set(stage.name for stage in stages) == {
        "copy-foo-foo1",
        "copy-foo1-foo2",
    }


def test_multi_stage_run_cached(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")

    run_copy("foo", "foo2", name="copy-foo1-foo2")
    stage2 = run_copy("foo", "foo2", name="copy-foo1-foo2")

    assert stage2 is None


def test_multistage_dump_on_non_cached_outputs(tmp_dir, dvc):
    tmp_dir.dvc_gen("foo")
    dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs_no_cache=["foo1"],
    )


def test_multistage_with_wdir(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    stage = dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs=["foo1"],
        wdir="dir",
    )

    data, _ = Dvcfile(dvc, stage.path)._load()
    assert "dir" == data["stages"]["copy-foo1-foo2"]["wdir"]


def test_multistage_always_changed(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    stage = dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs=["foo1"],
        always_changed=True,
    )

    data, _ = Dvcfile(dvc, stage.path)._load()
    assert data["stages"]["copy-foo1-foo2"]["always_changed"]


def test_graph(tmp_dir, dvc):
    from dvc.exceptions import CyclicGraphError

    tmp_dir.gen({"foo": "foo", "bar": "bar"})

    dvc.run(deps=["foo"], outs=["bar"], cmd="echo foo > bar", name="1")

    dvc.run(deps=["bar"], outs=["baz"], cmd="echo bar > baz", name="2")

    with pytest.raises(CyclicGraphError):
        dvc.run(deps=["baz"], outs=["foo"], cmd="echo baz > foo", name="3")


def test_run_dump_on_multistage(tmp_dir, dvc):
    from dvc.dvcfile import Dvcfile, PIPELINE_FILE

    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo-foo1",
        outs=["foo1"],
        wdir="dir",
    )
    data, _ = Dvcfile(dvc, PIPELINE_FILE)._load()
    assert data == {
        "stages": {
            "copy-foo-foo1": {
                "cmd": "cp foo foo1",
                "wdir": "dir",
                "deps": ["foo"],
                "outs": ["foo1"],
            }
        }
    }

    dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        wdir="dir",
        outs_persist=["foo2"],
        always_changed=True,
    )
    assert Dvcfile(dvc, PIPELINE_FILE)._load()[0] == {
        "stages": {
            "copy-foo-foo2": {
                "cmd": "cp foo foo2",
                "deps": ["foo"],
                "outs_persist": ["foo2"],
                "always_changed": True,
                "wdir": "dir",
            },
            **data["stages"],
        }
    }


def test_run_with_invalid_stage_name(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    with pytest.raises(InvalidStageName):
        run_copy("foo", "bar", name="email@https://dvc.org")


def test_run_already_exists(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy")
    with pytest.raises(DuplicateStageName):
        run_copy("bar", "foobar", name="copy")


supported_params = {
    "name": "Answer",
    "answer": 42,
    "floats": 42.0,
    "lists": [42, 42.0, "42"],
    "nested": {"nested1": {"nested2": "42", "nested2-2": 41.99999}},
}


def test_run_params_default(tmp_dir, dvc):
    from dvc.dependency import ParamsDependency

    with (tmp_dir / "params.yaml").open("w+") as f:
        yaml.dump(supported_params, f)

    stage = dvc.run(
        name="read_params",
        params=["nested.nested1.nested2"],
        cmd="cat params.yaml",
    )
    isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["nested.nested1.nested2"]

    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["read_params"]["params"] == {
        "params.yaml": {"nested.nested1.nested2": "42"}
    }

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [
        "nested.nested1.nested2"
    ]


def test_run_params_custom_file(tmp_dir, dvc):
    from dvc.dependency import ParamsDependency

    with (tmp_dir / "params2.yaml").open("w+") as f:
        yaml.dump(supported_params, f)

    stage = dvc.run(
        name="read_params",
        params=["params2.yaml:lists"],
        cmd="cat params2.yaml",
    )

    isinstance(stage.deps[0], ParamsDependency)
    assert stage.deps[0].params == ["lists"]
    lockfile = stage.dvcfile._lockfile
    assert lockfile.load()["read_params"]["params"] == {
        "params2.yaml": {"lists": [42, 42.0, "42"]}
    }

    data, _ = stage.dvcfile._load()
    assert data["stages"]["read_params"]["params"] == [
        {"params2.yaml": ["lists"]}
    ]
