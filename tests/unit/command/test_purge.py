import pytest

from dvc.cli import parse_args
from dvc.commands.purge import CmdPurge
from dvc.repo.purge import PurgeError


def test_purge_args_and_call(dvc, scm, mocker):
    cli_args = parse_args(
        [
            "purge",
            "foo",
            "bar",
            "--recursive",
            "--dry-run",
            "--force",
        ]
    )
    assert cli_args.func == CmdPurge

    cmd = cli_args.func(cli_args)
    mocker.patch("dvc.ui.ui.confirm", return_value=True)
    m = mocker.patch("dvc.repo.Repo.purge", return_value=0)

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["foo", "bar"],
        recursive=True,
        force=True,
        dry_run=True,
    )


def test_purge_defaults(mocker):
    cli_args = parse_args(["purge"])
    cmd = cli_args.func(cli_args)

    mocker.patch("dvc.ui.ui.confirm", return_value=True)
    m = mocker.patch("dvc.repo.Repo.purge", return_value=0)

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=[],
        recursive=False,
        force=False,
        dry_run=False,
    )


def test_purge_safety_error(mocker):
    cli_args = parse_args(["purge"])
    cmd = cli_args.func(cli_args)

    mocker.patch("dvc.ui.ui.confirm", return_value=True)
    m = mocker.patch("dvc.repo.Repo.purge", side_effect=PurgeError("dirty outs"))

    with pytest.raises(PurgeError):
        cmd.run()

    m.assert_called_once()
