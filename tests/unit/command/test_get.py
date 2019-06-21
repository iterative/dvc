from dvc.cli import parse_args
from dvc.command.get import CmdGet


def test_get(mocker):
    cli_args = parse_args(
        ["get", "repo_url", "src", "--out", "out", "--rev", "version"]
    )
    assert cli_args.func == CmdGet

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.get")

    assert cmd.run() == 0

    m.assert_called_once_with("repo_url", path="src", out="out", rev="version")
