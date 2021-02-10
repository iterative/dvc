import io
import posixpath

import pytest

from dvc.tree.gdrive import GDriveAuthError, GDriveTree

USER_CREDS_TOKEN_REFRESH_ERROR = '{"access_token": "", "client_id": "", "client_secret": "", "refresh_token": "", "token_expiry": "", "token_uri": "https://oauth2.googleapis.com/token", "user_agent": null, "revoke_uri": "https://oauth2.googleapis.com/revoke", "id_token": null, "id_token_jwt": null, "token_response": {"access_token": "", "expires_in": 3600, "scope": "https://www.googleapis.com/auth/drive.appdata https://www.googleapis.com/auth/drive", "token_type": "Bearer"}, "scopes": ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/drive.appdata"], "token_info_uri": "https://oauth2.googleapis.com/tokeninfo", "invalid": true, "_class": "OAuth2Credentials", "_module": "oauth2client.client"}'  # noqa: E501

USER_CREDS_MISSED_KEY_ERROR = "{}"


class TestRemoteGDrive:
    CONFIG = {
        "url": "gdrive://root/data",
        "gdrive_client_id": "client",
        "gdrive_client_secret": "secret",
    }

    def test_init(self, dvc):
        tree = GDriveTree(dvc, self.CONFIG)
        assert str(tree.path_info) == self.CONFIG["url"]

    def test_drive(self, dvc, monkeypatch):
        tree = GDriveTree(dvc, self.CONFIG)
        monkeypatch.setenv(
            GDriveTree.GDRIVE_CREDENTIALS_DATA, USER_CREDS_TOKEN_REFRESH_ERROR
        )
        with pytest.raises(GDriveAuthError):
            assert tree._drive

        monkeypatch.setenv(GDriveTree.GDRIVE_CREDENTIALS_DATA, "")
        tree = GDriveTree(dvc, self.CONFIG)
        monkeypatch.setenv(
            GDriveTree.GDRIVE_CREDENTIALS_DATA, USER_CREDS_MISSED_KEY_ERROR
        )
        with pytest.raises(GDriveAuthError):
            assert tree._drive


def test_gdrive_ls(dvc, gdrive, tmp_dir):
    tree = GDriveTree(dvc, gdrive.config)
    files = {
        "bar/baz/file0",
        "bar/file1",
        "foo/file2",
        "file3",
        "file4",
    }
    top_level_contents = {"bar", "foo", "file3", "file4"}

    for path in files:
        fobj = io.BytesIO(path.encode())
        tree.upload_fobj(fobj, gdrive / path)

    for recursive, expected in [(True, files), (False, top_level_contents)]:
        assert {
            posixpath.relpath(filename, gdrive.path)
            for filename in tree.ls(gdrive, recursive=recursive)
        } == expected
