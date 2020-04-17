import pytest
import os


def test_run_with_name(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage
    from dvc.dvcfile import DVC_FILE, DVC_FILE_SUFFIX

    tmp_dir.dvc_gen("foo", "foo")
    assert not os.path.exists(DVC_FILE)
    stage = run_copy("foo", "bar", name="copy-foo-to-bar")
    assert isinstance(stage, PipelineStage)
    assert stage.name == "copy-foo-to-bar"
    assert os.path.exists(DVC_FILE)
    assert os.path.exists(DVC_FILE + ".lock")
    assert os.path.exists("foo" + DVC_FILE_SUFFIX)


def test_run_with_multistage_and_single_stage(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage, Stage

    tmp_dir.dvc_gen("foo", "foo")
    stage1 = run_copy("foo", "foo1")
    stage2 = run_copy("foo1", "foo2", name="copy-foo1-foo2")
    stage3 = run_copy("foo2", "foo3")

    assert isinstance(stage2, PipelineStage)
    assert isinstance(stage1, Stage)
    assert isinstance(stage3, Stage)
    assert stage2.name == "copy-foo1-foo2"


def test_run_multi_stage_repeat(tmp_dir, dvc, run_copy):
    from dvc.stage import PipelineStage
    from dvc.dvcfile import Dvcfile, DVC_FILE, DVC_FILE_SUFFIX

    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "foo1", name="copy-foo-foo1")
    run_copy("foo1", "foo2", name="copy-foo1-foo2")
    run_copy("foo2", "foo3")

    stages = Dvcfile(dvc, DVC_FILE).load_multi()
    assert len(stages) == 2
    assert all(isinstance(stage, PipelineStage) for stage in stages)
    assert set(stage.name for stage in stages) == {
        "copy-foo-foo1",
        "copy-foo1-foo2",
    }
    assert all(
        os.path.exists(file + DVC_FILE_SUFFIX)
        for file in ["foo1", "foo2", "foo3"]
    )


def test_multi_stage_try_writing_on_single_stage_file(tmp_dir, dvc, run_copy):
    from dvc.exceptions import DvcException
    from dvc.dvcfile import MultiStageFileLoadError

    tmp_dir.dvc_gen("foo")
    dvc.run(cmd="echo foo", deps=["foo"])

    with pytest.raises(DvcException):
        run_copy("foo", "foo2", name="copy-foo1-foo2")

    run_copy("foo", "foo2", name="copy-foo1-foo2", fname="DIFFERENT-FILE.dvc")

    with pytest.raises(MultiStageFileLoadError):
        run_copy("foo2", "foo3", fname="DIFFERENT-FILE.dvc")


def test_multi_stage_run_cached(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")

    run_copy("foo", "foo2", name="copy-foo1-foo2")
    stage2 = run_copy("foo", "foo2", name="copy-foo1-foo2")

    assert stage2 is None


def test_multistage_dump_on_non_cached_outputs(tmp_dir, dvc):
    from dvc.dvcfile import DVC_FILE_SUFFIX

    tmp_dir.dvc_gen("foo")
    dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo1-foo2",
        outs_no_cache=["foo1"],
    )
    assert not os.path.exists("foo1" + DVC_FILE_SUFFIX)


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
    from dvc.dvcfile import Dvcfile

    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    dvc.run(
        cmd="cp foo foo1",
        deps=["foo"],
        name="copy-foo-foo1",
        outs=["foo1"],
        wdir="dir",
    )
    data, _ = Dvcfile(dvc, "Dvcfile")._load()
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
    assert Dvcfile(dvc, "Dvcfile")._load()[0] == {
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
