from dvc.cli import parse_args
from dvc.commands.du import CmdDU


def test_du(mocker):
    cli_args = parse_args(["du", "myurl", "mypath", "--summarize", "--rev", "myrev"])
    assert cli_args.func == CmdDU

    cmd = cli_args.func(cli_args)
    mock_du = mocker.patch("dvc.repo.Repo.du")

    assert cmd.run() == 0
    mock_du.assert_called_once_with(
        "myurl",
        "mypath",
        rev="myrev",
        summarize=True,
        config=None,
        remote=None,
        remote_config=None,
    )
