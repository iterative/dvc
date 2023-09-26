import textwrap

import pytest

from dvc.annotations import Annotation
from dvc.dvcfile import (
    LOCK_FILE,
    PROJECT_FILE,
    ParametrizedDumpError,
    SingleStageFile,
    load_file,
)
from dvc.stage.exceptions import StageFileDoesNotExistError
from dvc.stage.loader import StageNotFound
from dvc.utils.strictyaml import YAMLValidationError

STAGE_EXAMPLE = {
    "stage1": {
        "cmd": "cp foo bar",
        "desc": "stage desc",
        "meta": {"key1": "value1", "key2": "value2"},
        "deps": ["foo"],
        "outs": [{"bar": {"desc": "bar desc", "meta": {"key": "value"}}}],
    }
}


def test_run_load_one_for_multistage(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    stage1 = dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        outs_persist_no_cache=["foo2"],
        always_changed=True,
    )
    stage2 = load_file(dvc, PROJECT_FILE).stages["copy-foo-foo2"]
    assert stage1 == stage2
    foo_out = stage2.outs[0]
    assert stage2.cmd == "cp foo foo2"
    assert stage2.name == "copy-foo-foo2"
    assert foo_out.def_path == "foo2"
    assert foo_out.persist
    assert not foo_out.use_cache
    assert stage2.deps[0].def_path == "foo"
    assert dvc.reproduce(":copy-foo-foo2")


def test_run_load_one_for_multistage_non_existing(tmp_dir, dvc):
    with pytest.raises(StageFileDoesNotExistError):
        assert load_file(dvc, PROJECT_FILE).stages.get("copy-foo-foo2")


def test_run_load_one_for_multistage_non_existing_stage_name(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    stage = dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        metrics=["foo2"],
        always_changed=True,
    )
    with pytest.raises(StageNotFound):
        assert load_file(dvc, stage.path).stages["random-name"]


def test_run_load_one_on_single_stage(tmp_dir, dvc):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")
    assert isinstance(load_file(dvc, stage.path), SingleStageFile)
    assert load_file(dvc, stage.path).stages.get("random-name") == stage
    assert load_file(dvc, stage.path).stage == stage


def test_has_stage_with_name(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        metrics=["foo2"],
        always_changed=True,
    )
    dvcfile = load_file(dvc, PROJECT_FILE)
    assert "copy-foo-foo2" in dvcfile.stages
    assert "copy" not in dvcfile.stages


def test_load_all_multistage(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    stage1 = dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        metrics=["foo2"],
        always_changed=True,
    )
    stages = load_file(dvc, PROJECT_FILE).stages.values()
    assert len(stages) == 1
    assert list(stages) == [stage1]

    tmp_dir.gen("bar", "bar")
    stage2 = dvc.run(
        cmd="cp bar bar2",
        deps=["bar"],
        name="copy-bar-bar2",
        metrics=["bar2"],
        always_changed=True,
    )
    assert set(load_file(dvc, PROJECT_FILE).stages.values()) == {
        stage2,
        stage1,
    }


def test_load_all_singlestage(tmp_dir, dvc):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    dvcfile = load_file(dvc, "foo.dvc")
    assert isinstance(dvcfile, SingleStageFile)
    assert len(dvcfile.stages) == 1
    stages = dvcfile.stages.values()
    assert len(stages) == 1
    assert list(stages) == [stage1]


def test_try_get_single_stage_from_pipeline_file(tmp_dir, dvc):
    from dvc.dvcfile import DvcException

    tmp_dir.gen("foo", "foo")
    dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        metrics=["foo2"],
        always_changed=True,
    )
    with pytest.raises(DvcException):
        assert load_file(dvc, PROJECT_FILE).stage


def test_stage_collection(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {"file1": "file1", "file2": "file2"},
            "foo": "foo",
            "bar": "bar",
        }
    )
    (stage1,) = dvc.add("dir")
    stage2 = dvc.run(
        cmd="cp foo foo2",
        deps=["foo"],
        name="copy-foo-foo2",
        metrics=["foo2"],
        always_changed=True,
    )
    assert set(dvc.index.stages) == {stage1, stage2}


def test_remove_stage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")

    dvc_file = load_file(dvc, PROJECT_FILE)
    assert dvc_file.exists()
    assert {"copy-bar-foobar", "copy-foo-bar"} == set(
        dvc_file._load()[0]["stages"].keys()
    )

    dvc_file.remove_stage(stage)

    assert ["copy-bar-foobar"] == list(dvc_file._load()[0]["stages"].keys())

    # sanity check
    stage2.reload()

    # re-check to see if it fails if there's no stage entry
    dvc_file.remove_stage(stage)
    dvc_file.remove(force=True)
    # should not fail when there's no file at all.
    dvc_file.remove_stage(stage)


def test_remove_stage_lockfile(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    stage2 = run_copy("bar", "foobar", name="copy-bar-foobar")

    dvc_file = load_file(dvc, PROJECT_FILE)
    lock_file = dvc_file._lockfile
    assert dvc_file.exists()
    assert lock_file.exists()
    assert {"copy-bar-foobar", "copy-foo-bar"} == set(lock_file.load()["stages"].keys())
    lock_file.remove_stage(stage)

    assert ["copy-bar-foobar"] == list(lock_file.load()["stages"].keys())

    # sanity check
    stage2.reload()

    # re-check to see if it fails if there's no stage entry
    lock_file.remove_stage(stage)
    lock_file.remove()
    # should not fail when there's no file at all.
    lock_file.remove_stage(stage)


def test_remove_stage_dvcfiles(tmp_dir, dvc, run_copy):
    (stage,) = tmp_dir.dvc_gen("foo", "foo")

    dvc_file = load_file(dvc, stage.path)
    assert dvc_file.exists()
    dvc_file.remove_stage(stage)
    assert not dvc_file.exists()

    # re-check to see if it fails if there's no stage entry
    dvc_file.remove_stage(stage)
    dvc_file.remove(force=True)

    # should not fail when there's no file at all.
    dvc_file.remove_stage(stage)


def test_remove_stage_on_lockfile_format_error(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="copy-foo-bar")
    dvc_file = load_file(dvc, stage.path)
    lock_file = dvc_file._lockfile

    data = dvc_file._load()[0]
    lock_data = lock_file.load()
    lock_data["gibberish"] = True
    data["gibberish"] = True
    (tmp_dir / lock_file.relpath).dump(lock_data)
    with pytest.raises(YAMLValidationError):
        dvc_file.remove_stage(stage)

    lock_file.remove()
    dvc_file.dump(stage, update_pipeline=False)

    (tmp_dir / dvc_file.relpath).dump(data)
    with pytest.raises(YAMLValidationError):
        dvc_file.remove_stage(stage)


def test_remove_stage_preserves_comment(tmp_dir, dvc, run_copy):
    tmp_dir.gen(
        "dvc.yaml",
        textwrap.dedent(
            """\
            stages:
                generate-foo:
                    cmd: "echo foo > foo"
                    # This copies 'foo' text to 'foo' file.
                    outs:
                    - foo
                copy-foo-bar:
                    cmd: "python copy.py foo bar"
                    deps:
                    - foo
                    outs:
                    - bar"""
        ),
    )

    dvc.reproduce(PROJECT_FILE)

    dvc_file = load_file(dvc, PROJECT_FILE)

    assert dvc_file.exists()
    assert (tmp_dir / LOCK_FILE).exists()
    assert (tmp_dir / "foo").exists()
    assert (tmp_dir / "bar").exists()

    dvc_file.remove_stage(dvc_file.stages["copy-foo-bar"])
    assert (
        "# This copies 'foo' text to 'foo' file."
        in (tmp_dir / PROJECT_FILE).read_text()
    )


def test_remove_stage_removes_dvcfiles_if_no_stages_left(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    run_copy("foo", "bar", name="run_copy")

    dvc_file = load_file(dvc, PROJECT_FILE)

    assert dvc_file.exists()
    assert (tmp_dir / LOCK_FILE).exists()
    assert (tmp_dir / "foo").exists()

    dvc_file.remove_stage(dvc_file.stages["run_copy"])
    assert not dvc_file.exists()
    assert not (tmp_dir / LOCK_FILE).exists()


def test_dvcfile_dump_preserves_meta(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", name="run_copy")
    dvcfile = stage.dvcfile

    data = dvcfile._load()[0]
    metadata = {"name": "copy-file"}
    stage.meta = metadata
    data["stages"]["run_copy"]["meta"] = metadata

    dvcfile.dump(stage)
    assert dvcfile._load()[0] == data
    assert dvcfile._load()[0]["stages"]["run_copy"]["meta"] == metadata


def test_dvcfile_dump_preserves_desc(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage_desc = "test stage description"
    out_desc = "test out description"

    stage = run_copy("foo", "bar", name="run_copy", desc=stage_desc)
    dvcfile = stage.dvcfile

    data = dvcfile._load()[0]
    data["stages"]["run_copy"]["outs"][0] = {"bar": {"desc": out_desc}}
    (tmp_dir / dvcfile.path).dump(data)

    assert stage.desc == stage_desc
    stage.outs[0].annot.desc = out_desc
    dvcfile.dump(stage)
    loaded = dvcfile._load()[0]
    assert loaded == data
    assert loaded["stages"]["run_copy"]["desc"] == stage_desc
    assert loaded["stages"]["run_copy"]["outs"][0]["bar"]["desc"] == out_desc


def test_dvcfile_dump_preserves_comments(tmp_dir, dvc):
    text = textwrap.dedent(
        """\
        stages:
          generate-foo:
            cmd: echo foo > foo
            # This copies 'foo' text to 'foo' file.
            outs:
            - foo"""
    )
    tmp_dir.gen("dvc.yaml", text)
    stage = dvc.stage.load_one(name="generate-foo")
    stage.outs[0].use_cache = False
    dvcfile = stage.dvcfile

    dvcfile.dump(stage)
    assert dvcfile._load()[1] == (text + ":\n\tcache: false\n".expandtabs())


@pytest.mark.parametrize(
    "data, name",
    [
        ({"build-us": {"cmd": "echo ${foo}"}}, "build-us"),
        (
            {"build": {"foreach": ["us", "gb"], "do": {"cmd": "echo ${foo}"}}},
            "build@us",
        ),
    ],
)
def test_dvcfile_try_dumping_parametrized_stage(tmp_dir, dvc, data, name):
    (tmp_dir / "dvc.yaml").dump({"stages": data, "vars": [{"foo": "foobar"}]})

    stage = dvc.stage.load_one(name=name)
    dvcfile = stage.dvcfile

    with pytest.raises(ParametrizedDumpError) as exc:
        dvcfile.dump(stage)

    assert str(exc.value) == f"cannot dump a parametrized stage: '{name}'"


def test_dvcfile_load_dump_stage_with_desc_meta(tmp_dir, dvc):
    data = {"stages": STAGE_EXAMPLE}
    (tmp_dir / "dvc.yaml").dump(data)

    stage = dvc.stage.load_one(name="stage1")
    assert stage.meta == {"key1": "value1", "key2": "value2"}
    assert stage.desc == "stage desc"
    assert stage.outs[0].annot == Annotation(desc="bar desc", meta={"key": "value"})

    # sanity check
    stage.dump()
    assert (tmp_dir / "dvc.yaml").parse() == data


def test_dvcfile_load_with_plots(tmp_dir, dvc):
    (tmp_dir / "dvc.yaml").dump(
        {
            "plots": [
                {"path/to/plot": {"x": "value", "y": "value"}},
                {"path/to/another/plot": {"x": "value", "y": "value"}},
                {"path/to/empty/plot": None},
                "path/to/plot/str",
            ],
            "stages": STAGE_EXAMPLE,
        },
    )
    plots = list(dvc.plots.collect())
    top_level_plots = plots[0]["workspace"]["definitions"]["data"]["dvc.yaml"]["data"]
    assert all(
        name in top_level_plots for name in ("path/to/plot", "path/to/another/plot")
    )


def test_dvcfile_dos2unix(tmp_dir, dvc):
    from dvc_data.hashfile.hash import HashInfo

    (tmp_dir / "foo.dvc").dump({"outs": [{"md5": "abc123", "size": 3, "path": "foo"}]})
    orig_content = (tmp_dir / "foo.dvc").read_text()
    stage = dvc.stage.load_one("foo.dvc")
    assert stage.outs[0].hash_name == "md5-dos2unix"
    assert stage.outs[0].hash_info == HashInfo("md5-dos2unix", "abc123")
    stage.dump()
    assert (tmp_dir / "foo.dvc").read_text() == orig_content
