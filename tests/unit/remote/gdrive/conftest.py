from datetime import datetime, timedelta
import json
import mock

from google_auth_oauthlib.flow import InstalledAppFlow
import google.oauth2.credentials

import pytest

from dvc.repo import Repo
from dvc.remote.gdrive import RemoteGDrive
from dvc.remote.gdrive.client import GDriveClient
from dvc.remote.gdrive.utils import MIME_GOOGLE_APPS_FOLDER
from dvc.remote.gdrive.oauth2 import OAuth2


AUTHORIZATION = {"authorization": "Bearer MOCK_token"}
FOLDER = {"mimeType": MIME_GOOGLE_APPS_FOLDER}
FILE = {"mimeType": "not-a-folder"}

COMMON_KWARGS = {
    "data": None,
    "headers": AUTHORIZATION,
    "timeout": GDriveClient.TIMEOUT,
}


class Response:
    def __init__(self, data, status_code=200):
        self._data = data
        self.text = json.dumps(data) if isinstance(data, dict) else data
        self.status_code = status_code

    def json(self):
        return self._data


@pytest.fixture()
def repo():
    return Repo(".")


@pytest.fixture
def gdrive(repo, client):
    ret = RemoteGDrive(repo, {"url": "gdrive://root/data"})
    ret.client = client
    return ret


@pytest.fixture
def client():
    return GDriveClient(
        RemoteGDrive.SPACE_DRIVE,
        "test",
        RemoteGDrive.DEFAULT_CREDENTIALPATH,
        RemoteGDrive.SCOPE_DRIVE,
        "console",
    )


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    mocked = mock.Mock(return_value=Response("test"))
    monkeypatch.setattr("requests.sessions.Session.request", mocked)
    return mocked


@pytest.fixture()
def mocked_get_metadata(client, monkeypatch):
    mocked = mock.Mock(
        client.get_metadata,
        return_value=dict(id="root", name="root", **FOLDER),
    )
    monkeypatch.setattr(client, "get_metadata", mocked)
    return mocked


@pytest.fixture()
def mocked_search(client, monkeypatch):
    mocked = mock.Mock(client.search)
    monkeypatch.setattr(client, "search", mocked)
    return mocked


def _url(url):
    return GDriveClient.GOOGLEAPIS_BASE_URL + url


def _p(root, path):
    return RemoteGDrive.path_cls.from_parts(
        "gdrive", netloc=root, path="/" + path
    )


@pytest.fixture(autouse=True)
def fake_creds(monkeypatch):

    creds = google.oauth2.credentials.Credentials(
        token="MOCK_token",
        refresh_token="MOCK_refresh_token",
        token_uri="MOCK_token_uri",
        client_id="MOCK_client_id",
        client_secret="MOCK_client_secret",
        scopes=["MOCK_scopes"],
    )
    creds.expiry = datetime.now() + timedelta(days=1)

    mocked_flow = mock.Mock()
    mocked_flow.run_console.return_value = creds
    mocked_flow.run_local_server.return_value = creds

    monkeypatch.setattr(
        InstalledAppFlow,
        "from_client_secrets_file",
        classmethod(lambda *args, **kwargs: mocked_flow),
    )

    monkeypatch.setattr(
        OAuth2, "_get_creds_id", mock.Mock(return_value="test")
    )


@pytest.fixture(autouse=True)
def no_refresh(monkeypatch):
    expired_mock = mock.PropertyMock(return_value=False)
    monkeypatch.setattr(
        "google.oauth2.credentials.Credentials.expired", expired_mock
    )
    refresh_mock = mock.Mock()
    monkeypatch.setattr(
        "google.oauth2.credentials.Credentials.refresh", refresh_mock
    )
    return refresh_mock, expired_mock


@pytest.fixture()
def makedirs(gdrive, monkeypatch):
    mocked = mock.Mock(gdrive.makedirs, return_value="FOLDER_ID")
    monkeypatch.setattr(gdrive, "makedirs", mocked)
    return mocked
