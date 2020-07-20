import os
import posixpath

import configobj

from dvc.main import main
from dvc.repo import Repo
from dvc.tree.gdrive import GDriveTree


def test_relative_user_credentials_file_config_setting(tmp_dir, dvc):
    # CI sets it to test GDrive, here we want to test the work with file system
    # based, regular credentials
    if os.getenv(GDriveTree.GDRIVE_CREDENTIALS_DATA):
        del os.environ[GDriveTree.GDRIVE_CREDENTIALS_DATA]

    credentials = os.path.join("secrets", "credentials.json")

    # GDriveTree.credentials_location helper checks for file existence,
    # create the file
    tmp_dir.gen(credentials, "{'token': 'test'}")

    remote_name = "gdrive"
    assert (
        main(["remote", "add", "-d", remote_name, "gdrive://root/test"]) == 0
    )
    assert (
        main(
            [
                "remote",
                "modify",
                remote_name,
                "gdrive_user_credentials_file",
                credentials,
            ]
        )
        == 0
    )

    # We need to load repo again to test updates to the config
    str_path = os.fspath(tmp_dir)
    repo = Repo(str_path)

    # Check that in config we got the path relative to the config file itself
    # Also, we store posix path even on Windows
    config = configobj.ConfigObj(repo.config.files["repo"])
    assert config[f'remote "{remote_name}"'][
        "gdrive_user_credentials_file"
    ] == posixpath.join("..", "secrets", "credentials.json")

    # Check that in the remote itself we got an absolute path
    remote = repo.cloud.get_remote(remote_name)
    assert os.path.normpath(remote.tree.credentials_location) == os.path.join(
        str_path, credentials
    )
