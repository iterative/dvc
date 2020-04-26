import pytest

from dvc.exceptions import HTTPError
from dvc.path_info import URLInfo
from dvc.remote.http import HTTPRemote
from tests.utils.httpd import StaticFileServer


def test_download_fails_on_error_code(dvc):
    with StaticFileServer() as httpd:
        url = "http://localhost:{}/".format(httpd.server_port)
        config = {"url": url}

        remote = HTTPRemote(dvc, config)

        with pytest.raises(HTTPError):
            remote._download(URLInfo(url) / "missing.txt", "missing.txt")


def test_public_auth_method(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "user": "",
        "password": "",
    }

    remote = HTTPRemote(dvc, config)

    assert remote.auth_method() is None


def test_basic_auth_method(dvc):
    from requests.auth import HTTPBasicAuth

    user = "username"
    password = "password"
    auth = HTTPBasicAuth(user, password)
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "basic",
        "user": user,
        "password": password,
    }

    remote = HTTPRemote(dvc, config)

    assert remote.auth_method() == auth
    assert isinstance(remote.auth_method(), HTTPBasicAuth)


def test_digest_auth_method(dvc):
    from requests.auth import HTTPDigestAuth

    user = "username"
    password = "password"
    auth = HTTPDigestAuth(user, password)
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "digest",
        "user": user,
        "password": password,
    }

    remote = HTTPRemote(dvc, config)

    assert remote.auth_method() == auth
    assert isinstance(remote.auth_method(), HTTPDigestAuth)


def test_custom_auth_method(dvc):
    header = "Custom-Header"
    password = "password"
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "auth": "custom",
        "custom_auth_header": header,
        "password": password,
    }

    remote = HTTPRemote(dvc, config)

    assert remote.auth_method() is None
    assert header in remote.headers
    assert remote.headers[header] == password
