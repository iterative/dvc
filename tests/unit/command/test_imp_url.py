import logging
import os
from pathlib import Path

import pytest

from dvc.cli import parse_args
from dvc.command.imp_url import CmdImportUrl
from dvc.exceptions import DvcException
from dvc.tree import GDriveTree


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


def test_import_url_https(dvc):
    cli_args = parse_args(
        ["import-url", "https://data.dvc.org/get-started/data.xml"]
    )
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    assert cmd.run() == 0


# The first file id is a publicly available file.
# The second, instead, requires authentication, which depend on a proper
# gdrive-user-credentials.json. The absence of the file skips the test.
@pytest.mark.parametrize(
    "path, auth",
    [
        # ("1nKf4XcsNCN3oLujqlFTJoK5Fvx9iKCZb", False),
        # ("1syA-26p7tehWyUiMPPk_s0hsFN0Nr_kX", True),
        ("16onq6BZiiUFj083XloYVk7LDDpklDr7h/dir/data.txt", True),
        # ("16onq6BZiiUFj083XloYVk7LDDpklDr7h/dir", True),
    ],
)
def test_import_url_gdrive(dvc, path, auth):
    root_dir = Path(dvc.root_dir)

    if not os.getenv(GDriveTree.GDRIVE_CREDENTIALS_DATA):
        pytest.skip("no gdrive credentials data available")

    if not auth:
        os.environ[GDriveTree.GDRIVE_CREDENTIALS_DATA] = ""

    url = f"gdrive://{path}"
    cli_args = parse_args(["import-url", url])
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    res = cmd.run()
    assert res == 0

    if path.endswith("dir"):
        root_dir = root_dir.joinpath("dir")

    data_file = root_dir.joinpath("data.txt")
    assert data_file.exists()
    with open(data_file) as f:
        assert f.readline().strip() == "the data content"

    data_dvc_file = root_dir.joinpath("data.txt.dvc")
    assert data_dvc_file.exists()

    with open(data_dvc_file) as f:
        assert f.readlines()
