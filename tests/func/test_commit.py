from __future__ import unicode_literals

import pytest

from dvc.stage import StageCommitError
from dvc.utils.stage import dump_stage_file
from dvc.utils.stage import load_stage_file


def test_commit_recursive(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "subdir": {"file2": "text2"}}})
    stages = dvc.add("dir", recursive=True, no_commit=True)

    assert len(stages) == 2
    assert dvc.status() != {}

    dvc.commit("dir", recursive=True)
    assert dvc.status() == {}


def test_commit_force(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"file": "text1", "file2": "text2"}})
    stage, = dvc.add("dir", no_commit=True)

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


def test_commit_with_deps(tmp_dir, dvc, run_copy):
    tmp_dir.gen("foo", "foo")
    foo_stage, = dvc.add("foo", no_commit=True)
    assert foo_stage is not None
    assert len(foo_stage.outs) == 1

    stage = run_copy("foo", "file", no_commit=True)
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
    stage, = dvc.add("file", no_commit=True)

    stage_file_content = load_stage_file(stage.path)
    stage_file_content["md5"] = "1111111111"
    dump_stage_file(stage.path, stage_file_content)

    with pytest.raises(StageCommitError):
        dvc.commit(stage.path)

    dvc.commit(stage.path, force=True)
