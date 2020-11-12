import logging
from textwrap import dedent

import pytest
from funcy import first

import dvc as dvc_module
from dvc.exceptions import DvcException
from dvc.repo.experiments import Experiments

CHECKPOINT_SCRIPT_FORMAT = dedent(
    """\
    import os
    import sys
    import shutil
    from time import sleep

    from dvc.api import make_checkpoint

    checkpoint_file = {}
    checkpoint_iterations = int({})
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file) as fobj:
            try:
                value = int(fobj.read())
            except ValueError:
                value = 0
    else:
        with open(checkpoint_file, "w"):
            pass
        value = 0

    shutil.copyfile({}, {})

    if os.getenv("DVC_CHECKPOINT"):
        for _ in range(checkpoint_iterations):
            value += 1
            with open(checkpoint_file, "w") as fobj:
                fobj.write(str(value))
            make_checkpoint()
"""
)
CHECKPOINT_SCRIPT = CHECKPOINT_SCRIPT_FORMAT.format(
    "sys.argv[1]", "sys.argv[2]", "sys.argv[3]", "sys.argv[4]"
)


@pytest.fixture
def checkpoint_stage(tmp_dir, scm, dvc):
    tmp_dir.gen("checkpoint.py", CHECKPOINT_SCRIPT)
    tmp_dir.gen("params.yaml", "foo: 1")
    stage = dvc.run(
        cmd="python checkpoint.py foo 5 params.yaml metrics.yaml",
        metrics_no_cache=["metrics.yaml"],
        params=["foo"],
        checkpoints=["foo"],
        no_exec=True,
        name="checkpoint-file",
    )
    scm.add(["dvc.yaml", "checkpoint.py", "params.yaml"])
    scm.commit("init")
    return stage


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
def test_resume_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker, last):
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
    if last:
        exp_rev = Experiments.LAST_CHECKPOINT
    else:
        exp_rev = first(results)

    dvc.experiments.run(checkpoint_stage.addressing, checkpoint_resume=exp_rev)

    assert (tmp_dir / "foo").read_text() == "10"
    assert (
        tmp_dir / ".dvc" / "experiments" / "metrics.yaml"
    ).read_text().strip() == "foo: 2"


def test_reset_checkpoint(tmp_dir, scm, dvc, checkpoint_stage, mocker, caplog):
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
