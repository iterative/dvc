import logging

from dvc.cli import parse_args
from dvc.command.imp_url import CmdImportUrl
from dvc.exceptions import DvcException


def test_import_url(mocker):
    cli_args = parse_args(["import-url", "src", "out", "--file", "file"])
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with("src", out="out", fname="file", no_exec=False)


def test_failed_import_url(mocker, caplog):
    cli_args = parse_args(["import-url", "http://somesite.com/file_name"])
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    mocker.patch.object(cmd.repo, "imp_url", side_effect=DvcException("error"))
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_error = (
            "failed to import http://somesite.com/file_name. "
            "You could also try downloading it manually, and "
            "adding it with `dvc add`."
        )
        assert expected_error in caplog.text


def test_import_url_no_exec(mocker):
    cli_args = parse_args(
        ["import-url", "--no-exec", "src", "out", "--file", "file"]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with("src", out="out", fname="file", no_exec=True)
