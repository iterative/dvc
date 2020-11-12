import pytest

from dvc.cli import parse_args
from dvc.command.experiments import (
    CmdExperimentsDiff,
    CmdExperimentsGC,
    CmdExperimentsRun,
    CmdExperimentsShow,
)
from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import InvalidArgumentError

from .test_repro import default_arguments as repro_arguments


def test_experiments_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--all",
            "--show-json",
            "--show-md",
            "--old",
            "--precision",
            "10",
        ]
    )
    assert cli_args.func == CmdExperimentsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.diff.diff", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo, a_rev="HEAD~10", b_rev="HEAD~1", all=True
    )


def test_experiments_show(dvc, mocker):
    cli_args = parse_args(
        [
            "experiments",
            "show",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "--sha",
        ]
    )
    assert cli_args.func == CmdExperimentsShow

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.show.show", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        all_tags=True,
        all_branches=True,
        all_commits=True,
        sha_only=True,
    )


@pytest.mark.parametrize(
    "args, resume", [(["exp", "run"], None), (["exp", "resume"], ":last")]
)
def test_experiments_run(dvc, mocker, args, resume):
    default_arguments = {
        "params": [],
        "queue": False,
        "run_all": False,
        "jobs": None,
        "checkpoint_resume": resume,
    }

    default_arguments.update(repro_arguments)

    cmd = CmdExperimentsRun(parse_args(args))
    mocker.patch.object(cmd.repo, "reproduce")
    mocker.patch.object(cmd.repo.experiments, "run")
    cmd.run()
    cmd.repo.experiments.run.assert_called_with(
        PIPELINE_FILE, **default_arguments
    )


def test_experiments_gc(dvc, mocker):
    cli_args = parse_args(
        [
            "exp",
            "gc",
            "--workspace",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "--queued",
            "--force",
        ]
    )
    assert cli_args.func == CmdExperimentsGC

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.experiments.gc.gc", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        workspace=True,
        all_tags=True,
        all_branches=True,
        all_commits=True,
        queued=True,
    )

    cli_args = parse_args(["exp", "gc"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()
