from dvc.cli import parse_args
from dvc.commands.imp import CmdImport


def test_import(mocker, dvc):
    cli_args = parse_args(
        [
            "import",
            "repo_url",
            "src",
            "--out",
            "out",
            "--rev",
            "version",
            "--jobs",
            "3",
            "--config",
            "myconfig",
        ]
    )
    assert cli_args.func == CmdImport

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url",
        path="src",
        out="out",
        rev="version",
        no_exec=False,
        no_download=False,
        jobs=3,
        config="myconfig",
    )


def test_import_no_exec(mocker, dvc):
    cli_args = parse_args(
        [
            "import",
            "repo_url",
            "src",
            "--out",
            "out",
            "--rev",
            "version",
            "--no-exec",
        ]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url",
        path="src",
        out="out",
        rev="version",
        no_exec=True,
        no_download=False,
        jobs=None,
        config=None,
    )


def test_import_no_download(mocker, dvc):
    cli_args = parse_args(
        [
            "import",
            "repo_url",
            "src",
            "--out",
            "out",
            "--rev",
            "version",
            "--no-download",
        ]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "repo_url",
        path="src",
        out="out",
        rev="version",
        no_exec=False,
        no_download=True,
        jobs=None,
        config=None,
    )
