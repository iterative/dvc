import pytest
from funcy import first

from dvc.exceptions import DvcException
from dvc.repo.experiments import Experiments, MultipleBranchError


def test_new_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker):
    new_mock = mocker.spy(dvc.experiments, "new")
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    exp = first(results)

    new_mock.assert_called_once()
    tree = scm.get_tree(exp)
    with tree.open(tmp_dir / "foo") as fobj:
        assert fobj.read().strip() == "5"
    with tree.open(tmp_dir / "metrics.yaml") as fobj:
        assert fobj.read().strip() == "foo: 2"


@pytest.mark.parametrize(
    "checkpoint_resume", [Experiments.LAST_CHECKPOINT, "foo"]
)
def test_resume_checkpoint(
    tmp_dir, scm, dvc, checkpoint_stage, checkpoint_resume
):
    with pytest.raises(DvcException):
        dvc.experiments.run(
            checkpoint_stage=checkpoint_stage.addressing,
            checkpoint_resume=checkpoint_resume,
        )

    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )

    with pytest.raises(DvcException):
        dvc.experiments.run(
            checkpoint_stage.addressing, checkpoint_resume="abc1234",
        )

    if checkpoint_resume != Experiments.LAST_CHECKPOINT:
        checkpoint_resume = first(results)

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=checkpoint_resume
    )
    exp = first(results)

    tree = scm.get_tree(exp)
    with tree.open(tmp_dir / "foo") as fobj:
        assert fobj.read().strip() == "10"
    with tree.open(tmp_dir / "metrics.yaml") as fobj:
        assert fobj.read().strip() == "foo: 2"


def test_reset_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, caplog):
    from dvc.repo.experiments.base import CheckpointExistsError

    dvc.experiments.run(checkpoint_stage.addressing, name="foo")
    scm.repo.git.reset(hard=True)
    scm.repo.git.clean(force=True)

    with pytest.raises(CheckpointExistsError):
        dvc.experiments.run(
            checkpoint_stage.addressing, name="foo", params=["foo=2"]
        )

    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"], name="foo", force=True
    )
    exp = first(results)

    tree = scm.get_tree(exp)
    with tree.open(tmp_dir / "foo") as fobj:
        assert fobj.read().strip() == "5"
    with tree.open(tmp_dir / "metrics.yaml") as fobj:
        assert fobj.read().strip() == "foo: 2"


def test_resume_branch(tmp_dir, scm, dvc, checkpoint_stage):
    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )
    branch_rev = first(results)

    results = dvc.experiments.run(
        checkpoint_stage.addressing, checkpoint_resume=branch_rev
    )
    checkpoint_a = first(results)

    results = dvc.experiments.run(
        checkpoint_stage.addressing,
        checkpoint_resume=branch_rev,
        params=["foo=100"],
    )
    checkpoint_b = first(results)

    tree = scm.get_tree(checkpoint_a)
    with tree.open(tmp_dir / "foo") as fobj:
        assert fobj.read().strip() == "10"
    with tree.open(tmp_dir / "metrics.yaml") as fobj:
        assert fobj.read().strip() == "foo: 2"

    tree = scm.get_tree(checkpoint_b)
    with tree.open(tmp_dir / "foo") as fobj:
        assert fobj.read().strip() == "10"
    with tree.open(tmp_dir / "metrics.yaml") as fobj:
        assert fobj.read().strip() == "foo: 100"

    with pytest.raises(MultipleBranchError):
        dvc.experiments.get_branch_containing(branch_rev)

    assert branch_rev == dvc.experiments.scm.repo.git.merge_base(
        checkpoint_a, checkpoint_b
    )
