import logging

import pytest
from funcy import first

import dvc as dvc_module
from dvc.exceptions import DvcException
from dvc.repo.experiments import Experiments, MultipleBranchError


def test_new_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker):
    from dvc.env import DVC_CHECKPOINT, DVC_ROOT

    new_mock = mocker.spy(dvc.experiments, "new")
    env_mock = mocker.spy(dvc_module.stage.run, "_checkpoint_env")
    dvc.experiments.run(checkpoint_stage.addressing, params=["foo=2"])

    new_mock.assert_called_once()
    env_mock.assert_called_once()
    assert set(env_mock.return_value.keys()) == {DVC_CHECKPOINT, DVC_ROOT}
    assert (tmp_dir / "foo").read_text() == "5"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 2"


@pytest.mark.parametrize("last", [True, False])
def test_resume_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, last):
    with pytest.raises(DvcException):
        if last:
            dvc.experiments.run(
                checkpoint_stage.addressing,
                checkpoint_resume=Experiments.LAST_CHECKPOINT,
            )
        else:
            dvc.experiments.run(
                checkpoint_stage.addressing, checkpoint_resume="foo"
            )

    results = dvc.experiments.run(
        checkpoint_stage.addressing, params=["foo=2"]
    )

    with pytest.raises(DvcException):
        dvc.experiments.run(
            checkpoint_stage.addressing, checkpoint_resume="abc1234",
        )

    if last:
        exp_rev = Experiments.LAST_CHECKPOINT
    else:
        exp_rev = first(results)

    dvc.experiments.run(checkpoint_stage.addressing, checkpoint_resume=exp_rev)

    assert (tmp_dir / "foo").read_text() == "10"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 2"


def test_reset_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, caplog):
    dvc.experiments.run(checkpoint_stage.addressing)
    scm.repo.git.reset(hard=True)
    scm.repo.git.clean(force=True)

    with caplog.at_level(logging.ERROR):
        results = dvc.experiments.run(checkpoint_stage.addressing)
        assert len(results) == 0
        assert "already exists" in caplog.text

    dvc.experiments.run(checkpoint_stage.addressing, force=True)

    assert (tmp_dir / "foo").read_text() == "5"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 1"


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

    dvc.experiments.checkout(checkpoint_a)
    assert (tmp_dir / "foo").read_text() == "10"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 2"

    dvc.experiments.checkout(checkpoint_b)
    assert (tmp_dir / "foo").read_text() == "10"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 100"

    with pytest.raises(MultipleBranchError):
        dvc.experiments._get_branch_containing(branch_rev)

    branch_a = dvc.experiments._get_branch_containing(checkpoint_a)
    branch_b = dvc.experiments._get_branch_containing(checkpoint_b)
    assert branch_rev == dvc.experiments.scm.repo.git.merge_base(
        branch_a, branch_b, fork_point=True
    )
