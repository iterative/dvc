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
            "--cloud",
            "--remote",
            "origin",
            "--force",
            "--jobs",
            "3",
            "--projects",
            "project1",
            "project2",
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
        cloud=True,
        remote="origin",
        force=True,
        jobs=3,
        repos=["project1", "project2"],
    )

    cli_args = parse_args(["gc"])
    cmd = cli_args.func(cli_args)
    with pytest.raises(InvalidArgumentError):
        cmd.run()
