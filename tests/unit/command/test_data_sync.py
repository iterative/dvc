from dvc.cli import parse_args
from dvc.command.data_sync import CmdDataFetch, CmdDataPull


def test_fetch(mocker):
    cli_args = parse_args(
        [
            "fetch",
            "target.dvc",
            "-r",
            "remote",
            "-a",
            "-T",
            "-d",
            "-R",
            "--verify",
            "-j",
            "2",
        ]
    )
    assert cli_args.func == CmdDataFetch

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.fetch")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target.dvc"],
        remote="remote",
        all_branches=True,
        all_tags=True,
        with_deps=True,
        recursive=True,
        verify=True,
        jobs=2,
    )


def test_pull(mocker):
    cli_args = parse_args(
        [
            "pull",
            "target.dvc",
            "-r",
            "remote",
            "-a",
            "-T",
            "-d",
            "-R",
            "-f",
            "--verify",
            "-j",
            "2",
        ]
    )
    assert cli_args.func == CmdDataPull

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.pull")

    assert cmd.run() == 0

    m.assert_called_once_with(
        targets=["target.dvc"],
        remote="remote",
        all_branches=True,
        all_tags=True,
        with_deps=True,
        recursive=True,
        force=True,
        verify=True,
        jobs=2,
    )
