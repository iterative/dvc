import pytest

from dvc.cli import parse_args
from dvc.commands.gc import CmdGC
from dvc.exceptions import InvalidArgumentError


def test_(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "gc",
            "--workspace",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "--all-experiments",
            "--date",
            "2022-06-30",
            "--cloud",
            "--remote",
            "origin",
            "--force",
            "--jobs",
            "3",
            "--dry",
            "--projects",
            "project1",
            "project2",
            "--skip-failed",
        ]
    )
    assert cli_args.func == CmdGC

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.gc", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        workspace=True,
        all_tags=True,
        all_branches=True,
        all_commits=True,
        all_experiments=True,
        commit_date="2022-06-30",
        cloud=True,
        remote="origin",
        force=True,
        jobs=3,
        repos=["project1", "project2"],
        rev=None,
        num=None,
        not_in_remote=False,
        dry=True,
        skip_failed=True,
    )

    cli_args = parse_args(["gc"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()

    cli_args = parse_args(["gc", "--num", "2"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()

    cli_args = parse_args(["gc", "--cloud", "--not-in-remote"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()

    cli_args = parse_args(["gc", "--remote", "myremote"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()
