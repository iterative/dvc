import logging
from pathlib import Path

import pytest

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
    "file_id, auth",
    [
        ("1nKf4XcsNCN3oLujqlFTJoK5Fvx9iKCZb", False),
        ("1syA-26p7tehWyUiMPPk_s0hsFN0Nr_kX", True),
    ],
)
def test_import_url_gdrive(dvc, file_id, auth):
    root_dir = Path(dvc.root_dir)

    if auth:
        # accessing the tests folder
        parent_dir = Path(__file__).parent.parent.parent
        gdrive_credentials = parent_dir.joinpath(
            "gdrive-user-credentials.json"
        )

        if gdrive_credentials.exists():
            import shutil

            inner_tmp = root_dir.joinpath(".dvc", "tmp")
            inner_tmp.mkdir(exist_ok=True)
            shutil.copy(gdrive_credentials, inner_tmp)
        else:
            pytest.skip("no gdrive-user-credentials.json available")

    url = f"gdrive://{file_id}"
    cli_args = parse_args(["import-url", url, "data.txt"])
    assert cli_args.func == CmdImportUrl

    cmd = cli_args.func(cli_args)
    res = cmd.run()
    assert res == 0

    data_file = root_dir.joinpath("data.txt")
    assert data_file.exists()
    with open(data_file) as f:
        assert f.readline().strip() == "the data content"

    data_dvc_file = root_dir.joinpath("data.txt.dvc")
    with open(data_dvc_file) as f:
        assert f.readlines()

    assert data_dvc_file.exists()
