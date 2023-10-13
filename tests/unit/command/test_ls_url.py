from dvc.cli import parse_args
from dvc.commands.ls_url import CmdListUrl
from dvc.config import Config


def test_ls_url(mocker, M):
    cli_args = parse_args(["ls-url", "src"])
    assert cli_args.func == CmdListUrl
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.ls_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src", recursive=False, fs_config=None, config=M.instance_of(Config)
    )


def test_recursive(mocker, M):
    cli_args = parse_args(["ls-url", "-R", "src"])
    assert cli_args.func == CmdListUrl
    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.ls_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src", recursive=True, fs_config=None, config=M.instance_of(Config)
    )
