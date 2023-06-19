import logging

import pytest

from dvc.cli import parse_args
from dvc.commands.imp_url import CmdImportUrl
from dvc.exceptions import DvcException


def test_import_url(mocker, dvc):
    cli_args = parse_args(
        [
            "import-url",
            "src",
            "out",
            "--jobs",
            "4",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src",
        out="out",
        no_exec=False,
        no_download=False,
        remote=None,
        to_remote=False,
        jobs=4,
        force=False,
        version_aware=False,
    )


def test_failed_import_url(mocker, caplog, dvc):
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


@pytest.mark.parametrize(
    "flag,expected",
    [
        ("--no-exec", {"no_exec": True, "no_download": False}),
        ("--no-download", {"no_download": True, "no_exec": False}),
    ],
)
def test_import_url_no_exec_download_flags(mocker, flag, expected, dvc):
    cli_args = parse_args(
        [
            "import-url",
            flag,
            "src",
            "out",
        ]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src",
        out="out",
        remote=None,
        to_remote=False,
        jobs=None,
        force=False,
        version_aware=False,
        **expected,
    )


def test_import_url_to_remote(mocker, dvc):
    cli_args = parse_args(
        [
            "import-url",
            "s3://bucket/foo",
            "bar",
            "--to-remote",
            "--remote",
            "remote",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "s3://bucket/foo",
        out="bar",
        no_exec=False,
        no_download=False,
        remote="remote",
        to_remote=True,
        jobs=None,
        force=False,
        version_aware=False,
    )


@pytest.mark.parametrize("flag", ["--no-exec", "--no-download", "--version-aware"])
def test_import_url_to_remote_invalid_combination(dvc, mocker, caplog, flag):
    cli_args = parse_args(
        [
            "import-url",
            "s3://bucket/foo",
            "bar",
            "--to-remote",
            "--remote",
            "remote",
            flag,
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = (
            "--no-exec/--no-download/--version-aware cannot be combined with "
            "--to-remote"
        )
        assert expected_msg in caplog.text


def test_import_url_to_remote_flag(dvc, mocker, caplog):
    cli_args = parse_args(
        ["import-url", "s3://bucket/foo", "bar", "--remote", "remote"]
    )

    cmd = cli_args.func(cli_args)
    with caplog.at_level(logging.ERROR, logger="dvc"):
        assert cmd.run() == 1
        expected_msg = "--remote can't be used without --to-remote"
        assert expected_msg in caplog.text
