import os

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

    def test_drive(self, dvc):
        tree = GDriveTree(dvc, self.CONFIG)
        os.environ[
            GDriveTree.GDRIVE_CREDENTIALS_DATA
        ] = USER_CREDS_TOKEN_REFRESH_ERROR
        with pytest.raises(GDriveAuthError):
            assert tree._drive

        os.environ[GDriveTree.GDRIVE_CREDENTIALS_DATA] = ""
        tree = GDriveTree(dvc, self.CONFIG)
        os.environ[
            GDriveTree.GDRIVE_CREDENTIALS_DATA
        ] = USER_CREDS_MISSED_KEY_ERROR
        with pytest.raises(GDriveAuthError):
            assert tree._drive
