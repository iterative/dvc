from dvc.cli import parse_args
from dvc.commands.update import CmdUpdate


def test_update(dvc, mocker):
    cli_args = parse_args(
        [
            "update",
            "target1",
            "target2",
            "--rev",
            "REV",
            "--recursive",
            "-j",
            "8",
        ]
    )
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        rev="REV",
        recursive=True,
        to_remote=False,
        no_download=False,
        remote=None,
        jobs=8,
        all_=False,
    )


def test_update_to_remote(dvc, mocker):
    cli_args = parse_args(
        [
            "update",
            "target1",
            "target2",
            "--to-remote",
            "-j",
            "5",
            "-r",
            "remote",
            "--recursive",
        ]
    )
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target1", "target2"],
        rev=None,
        recursive=True,
        to_remote=True,
        no_download=False,
        remote="remote",
        jobs=5,
        all_=False,
    )


def test_update_all(dvc, mocker):
    cli_args = parse_args(
        [
            "update",
            "--all",
        ]
    )
    assert cli_args.func == CmdUpdate
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.update")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=[],
        rev=None,
        recursive=False,
        to_remote=False,
        no_download=False,
        remote=None,
        jobs=None,
        all_=True,
    )


def test_update_all_with_targets(dvc, mocker):
    cli_args = parse_args(["update", "--all", "target1", "target2"])
    cmd = cli_args.func(cli_args)
    assert cmd.run() == 1
