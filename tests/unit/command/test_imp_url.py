import logging

import pytest

from dvc.cli import parse_args
from dvc.commands.imp_url import CmdImportUrl
from dvc.exceptions import DvcException


def test_import_url(mocker):
    cli_args = parse_args(
        [
            "import-url",
            "src",
            "out",
            "--file",
            "file",
            "--jobs",
            "4",
            "--desc",
            "description",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src",
        out="out",
        fname="file",
        no_exec=False,
        no_download=False,
        remote=None,
        to_remote=False,
        desc="description",
        type=None,
        labels=None,
        meta=None,
        jobs=4,
        version_aware=False,
    )


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


@pytest.mark.parametrize(
    "flag,expected",
    [
        ("--no-exec", {"no_exec": True, "no_download": False}),
        ("--no-download", {"no_download": True, "no_exec": False}),
    ],
)
def test_import_url_no_exec_download_flags(mocker, flag, expected):
    cli_args = parse_args(
        [
            "import-url",
            flag,
            "src",
            "out",
            "--file",
            "file",
            "--desc",
            "description",
        ]
    )

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "src",
        out="out",
        fname="file",
        remote=None,
        to_remote=False,
        desc="description",
        jobs=None,
        type=None,
        labels=None,
        meta=None,
        version_aware=False,
        **expected,
    )


def test_import_url_to_remote(mocker):
    cli_args = parse_args(
        [
            "import-url",
            "s3://bucket/foo",
            "bar",
            "--to-remote",
            "--remote",
            "remote",
            "--desc",
            "description",
        ]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "imp_url", autospec=True)

    assert cmd.run() == 0

    m.assert_called_once_with(
        "s3://bucket/foo",
        out="bar",
        fname=None,
        no_exec=False,
        no_download=False,
        remote="remote",
        to_remote=True,
        desc="description",
        type=None,
        labels=None,
        meta=None,
        jobs=None,
        version_aware=False,
    )


@pytest.mark.parametrize("flag", ["--no-exec", "--no-download"])
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
        expected_msg = "--no-exec/--no-download cannot be combined with --to-remote"
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
