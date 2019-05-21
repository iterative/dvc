import mock

import requests

import pytest

from dvc.remote.gdrive.exceptions import GDriveError, GDriveResourceNotFound

from tests.unit.remote.gdrive.conftest import (
    COMMON_KWARGS,
    FOLDER,
    FILE,
    Response,
    _url,
    _p,
)


def test_request(client, no_requests):
    assert client.request("GET", "test").text == "test"
    no_requests.assert_called_once_with("GET", _url("test"), **COMMON_KWARGS)


def test_request_refresh(client, no_requests, no_refresh):
    refresh_mock, _ = no_refresh
    no_requests.side_effect = [
        Response("error", 401),
        Response("after_refresh", 200),
    ]
    assert client.request("GET", "test").text == "after_refresh"
    refresh_mock.assert_called_once()
    assert no_requests.mock_calls == [
        mock.call("GET", _url("test"), **COMMON_KWARGS),
        mock.call("GET", _url("test"), **COMMON_KWARGS),
    ]


def test_request_expired(client, no_requests, no_refresh):
    refresh_mock, expired_mock = no_refresh
    expired_mock.side_effect = [True, False]
    no_requests.side_effect = [Response("test", 200)]
    assert client.request("GET", "test").text == "test"
    expired_mock.assert_called()
    refresh_mock.assert_called_once()
    assert no_requests.mock_calls == [
        mock.call("GET", _url("test"), **COMMON_KWARGS)
    ]


def test_request_retry_and_backoff(client, no_requests, monkeypatch):
    no_requests.side_effect = [
        Response("error", 500),
        Response("error", 500),
        Response("retry", 200),
    ]
    sleep_mock = mock.Mock()
    monkeypatch.setattr("dvc.remote.gdrive.client.sleep", sleep_mock)
    assert client.request("GET", "test").text == "retry"
    assert no_requests.mock_calls == [
        mock.call("GET", _url("test"), **COMMON_KWARGS),
        mock.call("GET", _url("test"), **COMMON_KWARGS),
        mock.call("GET", _url("test"), **COMMON_KWARGS),
    ]
    assert sleep_mock.mock_calls == [mock.call(1), mock.call(2)]


def test_request_4xx(client, no_requests):
    no_requests.return_value = Response("error", 400)
    with pytest.raises(GDriveError):
        client.request("GET", "test")


def test_search(client, no_requests):
    no_requests.side_effect = [
        Response({"files": ["test1"], "nextPageToken": "TEST_nextPageToken"}),
        Response({"files": ["test2"]}),
    ]
    assert list(client.search("test", "root")) == ["test1", "test2"]


def test_get_metadata(client, no_requests):
    no_requests.side_effect = [
        Response(dict(id="root", name="root", **FOLDER)),
        Response({"files": [dict(id="id1", name="path1", **FOLDER)]}),
        Response({"files": [dict(id="id2", name="path2", **FOLDER)]}),
    ]
    client.get_metadata(_p("root", "path1/path2"), ["field1", "field2"])
    assert no_requests.mock_calls == [
        mock.call("GET", _url("drive/v3/files/root"), **COMMON_KWARGS),
        mock.call(
            "GET",
            _url("drive/v3/files"),
            params={
                "q": "'root' in parents and name = 'path1'",
                "spaces": "drive",
            },
            **COMMON_KWARGS
        ),
        mock.call(
            "GET",
            _url("drive/v3/files"),
            params={
                "q": "'id1' in parents and name = 'path2'",
                "spaces": "drive",
                "fields": "files(field1,field2)",
            },
            **COMMON_KWARGS
        ),
    ]


def test_get_metadata_not_a_folder(client, no_requests, mocked_search):
    no_requests.return_value = Response(dict(id="id1", name="root", **FOLDER))
    mocked_search.return_value = [dict(id="id2", name="path1", **FILE)]
    with pytest.raises(GDriveError):
        client.get_metadata(_p("root", "path1/path2"), ["field1", "field2"])
    client.get_metadata(_p("root", "path1"), ["field1", "field2"])


def test_get_metadata_duplicate(client, no_requests, mocked_search):
    no_requests.return_value = Response(dict(id="id1", name="root", **FOLDER))
    mocked_search.return_value = [
        dict(id="id2", name="path1", **FOLDER),
        dict(id="id3", name="path1", **FOLDER),
    ]
    with pytest.raises(GDriveError):
        client.get_metadata(_p("root", "path1/path2"), ["field1", "field2"])


def test_get_metadata_not_found(client, no_requests, mocked_search):
    no_requests.return_value = Response(dict(id="root", name="root", **FOLDER))
    mocked_search.return_value = []
    with pytest.raises(GDriveResourceNotFound):
        client.get_metadata(_p("root", "path1/path2"), ["field1", "field2"])


def test_resumable_upload_first_request(client, no_requests):
    resp = Response("", 201)
    no_requests.return_value = resp
    from_file = mock.Mock()
    to_info = mock.Mock()
    assert (
        client._resumable_upload_first_request("url", from_file, to_info, 100)
        is True
    )


def test_resumable_upload_first_request_connection_error(client, no_requests):
    no_requests.side_effect = requests.ConnectionError
    from_file = mock.Mock()
    to_info = mock.Mock()
    assert (
        client._resumable_upload_first_request("url", from_file, to_info, 100)
        is False
    )


def test_resumable_upload_first_request_failure(client, no_requests):
    no_requests.return_value = Response("", 400)
    from_file = mock.Mock()
    to_info = mock.Mock()
    assert (
        client._resumable_upload_first_request("url", from_file, to_info, 100)
        is False
    )
