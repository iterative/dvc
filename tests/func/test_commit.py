import pytest
from dvc.utils.stage import load_stage_file, dump_stage_file

from dvc.stage import StageCommitError


def test_commit_recursive(dvc_repo, repo_dir):
    stages = dvc_repo.add(repo_dir.DATA_DIR, recursive=True, no_commit=True)
    assert len(stages) == 2

    assert dvc_repo.status() != {}

    dvc_repo.commit(repo_dir.DATA_DIR, recursive=True)

    assert dvc_repo.status() == {}


def test_commit_force(dvc_repo, repo_dir):
    stages = dvc_repo.add(repo_dir.FOO, no_commit=True)
    assert len(stages) == 1
    stage = stages[0]

    with dvc_repo.state:
        assert stage.outs[0].changed_cache()

    with open(repo_dir.FOO, "a") as fobj:
        fobj.write(repo_dir.FOO_CONTENTS)

    with dvc_repo.state:
        assert stage.outs[0].changed_cache()

    with pytest.raises(StageCommitError):
        dvc_repo.commit(stage.path)

    with dvc_repo.state:
        assert stage.outs[0].changed_cache()

    dvc_repo.commit(stage.path, force=True)

    assert dvc_repo.status([stage.path]) == {}


def test_commit_with_deps(dvc_repo, repo_dir):
    stages = dvc_repo.add(repo_dir.FOO, no_commit=True)
    assert len(stages) == 1
    foo_stage = stages[0]
    assert foo_stage is not None
    assert len(foo_stage.outs) == 1

    fname = "file"
    stage = dvc_repo.run(
        cmd="python {} {} {}".format(repo_dir.CODE, repo_dir.FOO, fname),
        outs=[fname],
        deps=[repo_dir.FOO, repo_dir.CODE],
        no_commit=True,
    )
    assert stage is not None
    assert len(stage.outs) == 1

    with dvc_repo.state:
        assert foo_stage.outs[0].changed_cache()
        assert stage.outs[0].changed_cache()

    dvc_repo.commit(stage.path, with_deps=True)
    with dvc_repo.state:
        assert not foo_stage.outs[0].changed_cache()
        assert not stage.outs[0].changed_cache()


def test_commit_changed_md5(dvc_repo, repo_dir):
    stages = dvc_repo.add(repo_dir.FOO, no_commit=True)
    assert len(stages) == 1
    stage = stages[0]

    stage_file_content = load_stage_file(stage.path)
    stage_file_content["md5"] = "1111111111"
    dump_stage_file(stage.path, stage_file_content)

    with pytest.raises(StageCommitError):
        dvc_repo.commit(stage.path)

    dvc_repo.commit(stage.path, force=True)
