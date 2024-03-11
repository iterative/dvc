from dvc.cli import parse_args
from dvc.commands.get import CmdGet


def test_get(mocker):
    cli_args = parse_args(
        [
            "get",
            "repo_url",
            "src",
            "--out",
            "out",
            "--rev",
            "version",
            "--jobs",
            "4",
            "--config",
            "myconfig",
            "--remote",
            "myremote",
            "--remote-config",
            "k1=v1",
            "k2=v2",
        ]
    )
    assert cli_args.func == CmdGet

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.Repo.get")

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url",
        path="src",
        out="out",
        rev="version",
        jobs=4,
        config="myconfig",
        force=False,
        remote="myremote",
        remote_config={"k1": "v1", "k2": "v2"},
    )


def test_get_url(mocker, capsys):
    cli_args = parse_args(
        [
            "get",
            "repo_url",
            "src",
            "--rev",
            "version",
            "--remote",
            "myremote",
            "--show-url",
            "--remote-config",
            "k1=v1",
            "k2=v2",
        ]
    )
    assert cli_args.func == CmdGet

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.api.get_url", return_value="resource_url")

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert "resource_url" in out

    m.assert_called_once_with(
        "src",
        repo="repo_url",
        rev="version",
        remote="myremote",
        remote_config={"k1": "v1", "k2": "v2"},
    )
