from dvc.cli import parse_args
from dvc.command.experiments import (
    CmdExperimentsDiff,
    CmdExperimentsRun,
    CmdExperimentsShow,
)
from dvc.dvcfile import PIPELINE_FILE


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


default_run_arguments = {
    "all_pipelines": False,
    "downstream": False,
    "dry": False,
    "force": False,
    "run_cache": True,
    "interactive": False,
    "no_commit": False,
    "pipeline": False,
    "single_item": False,
    "recursive": False,
    "force_downstream": False,
    "params": [],
    "queue": False,
    "run_all": False,
    "jobs": None,
    "checkpoint": False,
    "checkpoint_continue": None,
    "experiment": True,
}


def test_experiments_run(dvc, mocker):
    cmd = CmdExperimentsRun(parse_args(["exp", "run"]))
    mocker.patch.object(cmd.repo, "reproduce")
    cmd.run()
    cmd.repo.reproduce.assert_called_with(
        PIPELINE_FILE, **default_run_arguments
    )
