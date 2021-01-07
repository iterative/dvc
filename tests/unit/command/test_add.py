import logging

from dvc.cli import parse_args
from dvc.command.add import CmdAdd


def test_add(mocker, dvc):
    cli_args = parse_args(
        [
            "add",
            "--recursive",
            "--no-commit",
            "--external",
            "--glob",
            "--file",
            "file",
            "--desc",
            "stage description",
            "data",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "add", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        ["data"],
        recursive=True,
        no_commit=True,
        glob=True,
        fname="file",
        external=True,
        out=None,
        remote=None,
        straight_to_remote=False,
        desc="stage description",
    )


def test_add_straight_to_remote(mocker):
    cli_args = parse_args(
        [
            "add",
            "s3://bucket/foo",
            "--straight-to-remote",
            "--out",
            "bar",
            "--remote",
            "remote",
        ]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "add", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        ["s3://bucket/foo"],
        recursive=False,
        no_commit=False,
        glob=False,
        fname=None,
        external=False,
        out="bar",
        remote="remote",
        straight_to_remote=True,
        desc=None,
    )


def test_add_straight_to_remote_invalid_combinations(mocker, caplog):
    cli_args = parse_args(
        ["add", "s3://bucket/foo", "s3://bucket/bar", "--straight-to-remote"]
    )
    assert cli_args.func == CmdAdd

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "--straight-to-remote can't used with multiple targets"
        assert expected_msg in caplog.text

    for option in "--remote", "--out":
        cli_args = parse_args(["add", "foo", option, "bar"])

        cmd = cli_args.func(cli_args)
        with caplog.at_level(logging.ERROR, logger="dvc"):
            assert cmd.run() == 1
            expected_msg = (
                f"--{option} can't be used without --straight-to-remote"
            )
            assert expected_msg in caplog.text
