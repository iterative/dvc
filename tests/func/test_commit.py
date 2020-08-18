from os import fspath

import pytest

from dvc.dependency.base import DependencyDoesNotExistError
from dvc.dvcfile import PIPELINE_FILE
from dvc.output.base import OutputDoesNotExistError
from dvc.stage.exceptions import StageCommitError
from dvc.utils.serialize import dump_yaml, load_yaml


def test_commit_recursive(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "subdir": {"file2": "text2"}}})
    stages = dvc.add("dir", recursive=True, no_commit=True)

    assert len(stages) == 2
    assert dvc.status() != {}

    dvc.commit("dir", recursive=True)
    assert dvc.status() == {}


def test_commit_force(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "file2": "text2"}})
    (stage,) = dvc.add("dir", no_commit=True)

    with dvc.state:
        assert stage.outs[0].changed_cache()

    tmp_dir.gen("dir/file", "file content modified")

    with dvc.state:
        assert stage.outs[0].changed_cache()

    with pytest.raises(StageCommitError):
        dvc.commit(stage.path)

    with dvc.state:
        assert stage.outs[0].changed_cache()

    dvc.commit(stage.path, force=True)
    assert dvc.status([stage.path]) == {}


@pytest.mark.parametrize("run_kw", [{"single_stage": True}, {"name": "copy"}])
def test_commit_with_deps(tmp_dir, dvc, run_copy, run_kw):
    tmp_dir.gen("foo", "foo")
    (foo_stage,) = dvc.add("foo", no_commit=True)
    assert foo_stage is not None
    assert len(foo_stage.outs) == 1

    stage = run_copy("foo", "file", no_commit=True, **run_kw)
    assert stage is not None
    assert len(stage.outs) == 1

    with dvc.state:
        assert foo_stage.outs[0].changed_cache()
        assert stage.outs[0].changed_cache()

    dvc.commit(stage.path, with_deps=True)
    with dvc.state:
        assert not foo_stage.outs[0].changed_cache()
        assert not stage.outs[0].changed_cache()


def test_commit_changed_md5(tmp_dir, dvc):
    tmp_dir.gen({"file": "file content"})
    (stage,) = dvc.add("file", no_commit=True)

    stage_file_content = load_yaml(stage.path)
    stage_file_content["md5"] = "1111111111"
    dump_yaml(stage.path, stage_file_content)

    with pytest.raises(StageCommitError):
        dvc.commit(stage.path)

    dvc.commit(stage.path, force=True)
    assert "md5" not in load_yaml(stage.path)


def test_commit_no_exec(tmp_dir, dvc):
    tmp_dir.gen({"dep": "dep", "out": "out"})
    stage = dvc.run(
        name="my", cmd="mycmd", deps=["dep"], outs=["out"], no_exec=True
    )
    assert dvc.status(stage.path)
    dvc.commit(stage.path, force=True)
    assert dvc.status(stage.path) == {}


def test_commit_no_exec_missing_dep(tmp_dir, dvc):
    stage = dvc.run(
        name="my", cmd="mycmd", deps=["dep"], outs=["out"], no_exec=True
    )
    assert dvc.status(stage.path)

    with pytest.raises(DependencyDoesNotExistError):
        dvc.commit(stage.path, force=True)


def test_commit_no_exec_missing_out(tmp_dir, dvc):
    stage = dvc.run(name="my", cmd="mycmd", outs=["out"], no_exec=True)
    assert dvc.status(stage.path)

    with pytest.raises(OutputDoesNotExistError):
        dvc.commit(stage.path, force=True)


def test_commit_pipeline_stage(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    stage = run_copy("foo", "bar", no_commit=True, name="copy-foo-bar")
    assert dvc.status(stage.addressing)
    assert dvc.commit(stage.addressing, force=True) == [stage]
    assert not dvc.status(stage.addressing)

    # just to confirm different variants work
    assert dvc.commit(f":{stage.addressing}") == [stage]
    assert dvc.commit(f"{PIPELINE_FILE}:{stage.addressing}") == [stage]
    assert dvc.commit(PIPELINE_FILE) == [stage]


def test_imported_entries_unchanged(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.dvc_gen("file", "file content", "initial commit")

    stage = dvc.imp(fspath(erepo_dir), "file")

    with dvc.state:
        assert stage.changed_entries() == ([], [], None)
