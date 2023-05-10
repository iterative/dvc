import pytest
from funcy import first

from dvc.dependency.base import DependencyDoesNotExistError
from dvc.exceptions import ReproductionError


@pytest.mark.parametrize("tmp", [True, False])
@pytest.mark.parametrize("staged", [True, False])
def test_deleted(tmp_dir, scm, dvc, tmp, staged):
    tmp_dir.scm_gen("file", "file", commit="commit file")
    stage = dvc.stage.add(
        cmd="cat file",
        deps=["file"],
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    file = tmp_dir / "file"
    file.unlink()
    if staged:
        scm.add(["file"])

    with pytest.raises(ReproductionError) as exc_info:
        dvc.experiments.run(stage.addressing, tmp_dir=tmp)

    cause = exc_info._excinfo[1].__cause__
    assert isinstance(cause, DependencyDoesNotExistError)
    assert not file.exists()


@pytest.mark.parametrize("tmp", [True, False])
@pytest.mark.parametrize("staged", [True, False])
def test_modified(tmp_dir, scm, dvc, caplog, tmp, staged):
    tmp_dir.scm_gen("file", "file", commit="commit file")
    stage = dvc.stage.add(
        cmd="cat file",
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    (tmp_dir / "file").write_text("modified_file")
    if staged:
        scm.add(["file"])

    results = dvc.experiments.run(stage.addressing, tmp_dir=tmp)

    exp = first(results)
    scm.checkout(exp, force=True)
    assert (tmp_dir / "file").read_text() == "modified_file"


@pytest.mark.parametrize("tmp", [True, False])
def test_staged_new_file(tmp_dir, scm, dvc, tmp):
    stage = dvc.stage.add(
        cmd="cat file",
        name="foo",
    )
    scm.add_commit(["dvc.yaml"], message="add dvc.yaml")

    (tmp_dir / "file").write_text("file")
    scm.add(["file"])

    results = dvc.experiments.run(stage.addressing, tmp_dir=tmp)
    exp = first(results)
    fs = scm.get_fs(exp)
    assert fs.exists("file")
