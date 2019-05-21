import mock

import pytest

from dvc.remote.gdrive import RemoteGDrive, GDriveError, GDriveResourceNotFound

from tests.unit.remote.gdrive.conftest import (
    Response,
    FOLDER,
    FILE,
    COMMON_KWARGS,
    _p,
    _url,
)


def test_init_drive(repo):
    url = "gdrive://root/data"
    gdrive = RemoteGDrive(repo, {"url": url})
    assert gdrive.root == "root"
    assert str(gdrive.path_info) == url
    assert gdrive.client.scopes == ["https://www.googleapis.com/auth/drive"]
    assert gdrive.client.space == RemoteGDrive.SPACE_DRIVE


def test_init_appfolder(repo):
    url = "gdrive://appdatafolder/data"
    gdrive = RemoteGDrive(repo, {"url": url})
    assert gdrive.root == "appdatafolder"
    assert str(gdrive.path_info) == url
    assert gdrive.client.scopes == [
        "https://www.googleapis.com/auth/drive.appdata"
    ]
    assert gdrive.client.space == RemoteGDrive.SPACE_APPDATA


def test_init_folder_id(repo):
    url = "gdrive://folder_id/data"
    gdrive = RemoteGDrive(repo, {"url": url})
    assert gdrive.root == "folder_id"
    assert str(gdrive.path_info) == url
    assert gdrive.client.scopes == ["https://www.googleapis.com/auth/drive"]
    assert gdrive.client.space == "drive"


def test_get_file_checksum(gdrive, mocked_get_metadata):
    mocked_get_metadata.return_value = dict(
        id="id1", name="path1", md5Checksum="checksum"
    )
    checksum = gdrive.get_file_checksum(_p(gdrive.root, "path1"))
    assert checksum == "checksum"
    mocked_get_metadata.assert_called_once_with(
        _p(gdrive.root, "path1"), fields=["md5Checksum"]
    )


def test_list_cache_paths(gdrive, mocked_get_metadata, mocked_search):
    mocked_get_metadata.return_value = dict(id="root", name="root", **FOLDER)
    mocked_search.side_effect = [
        [dict(id="f1", name="f1", **FOLDER), dict(id="f2", name="f2", **FILE)],
        [dict(id="f3", name="f3", **FILE)],
    ]
    assert list(gdrive.list_cache_paths()) == ["data/f1/f3", "data/f2"]
    mocked_get_metadata.assert_called_once_with(_p("root", "data"))


def test_list_cache_path_not_found(gdrive, mocked_get_metadata):
    mocked_get_metadata.side_effect = GDriveResourceNotFound("test")
    assert list(gdrive.list_cache_paths()) == []
    mocked_get_metadata.assert_called_once_with(_p("root", "data"))


def test_mkdir(gdrive, no_requests):
    no_requests.return_value = Response("test")
    assert gdrive.mkdir("root", "test") == "test"
    no_requests.assert_called_once_with(
        "POST",
        _url("drive/v3/files"),
        json={
            "name": "test",
            "mimeType": FOLDER["mimeType"],
            "parents": ["root"],
            "spaces": "drive",
        },
        **COMMON_KWARGS
    )


def test_makedirs(gdrive, monkeypatch, mocked_get_metadata):
    mocked_get_metadata.side_effect = [
        dict(id="id1", name="test1", **FOLDER),
        GDriveResourceNotFound("test1/test2"),
    ]
    monkeypatch.setattr(
        gdrive, "mkdir", mock.Mock(side_effect=[{"id": "id2"}])
    )
    assert gdrive.makedirs(_p(gdrive.root, "test1/test2")) == "id2"
    assert mocked_get_metadata.mock_calls == [
        mock.call(_p(gdrive.root, "test1")),
        mock.call(_p("id1", "test2")),
    ]
    assert gdrive.mkdir.mock_calls == [mock.call("id1", "test2")]


def test_makedirs_error(gdrive, mocked_get_metadata):
    mocked_get_metadata.side_effect = [dict(id="id1", name="test1", **FILE)]
    with pytest.raises(GDriveError):
        gdrive.makedirs(_p(gdrive.root, "test1/test2"))
