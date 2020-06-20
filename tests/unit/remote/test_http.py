import pytest

from dvc.exceptions import HTTPError
from dvc.path_info import URLInfo
from dvc.remote.http import HTTPRemoteTree
from tests.utils.httpd import StaticFileServer


def test_download_fails_on_error_code(dvc):
    with StaticFileServer() as httpd:
        url = f"http://localhost:{httpd.server_port}/"
        config = {"url": url}

        tree = HTTPRemoteTree(dvc, config)

        with pytest.raises(HTTPError):
            tree._download(URLInfo(url) / "missing.txt", "missing.txt")


def test_public_auth_method(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "user": "",
        "password": "",
    }

    tree = HTTPRemoteTree(dvc, config)

    assert tree._auth_method() is None


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

    tree = HTTPRemoteTree(dvc, config)

    assert tree._auth_method() == auth
    assert isinstance(tree._auth_method(), HTTPBasicAuth)


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

    tree = HTTPRemoteTree(dvc, config)

    assert tree._auth_method() == auth
    assert isinstance(tree._auth_method(), HTTPDigestAuth)


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

    tree = HTTPRemoteTree(dvc, config)

    assert tree._auth_method() is None
    assert header in tree.headers
    assert tree.headers[header] == password
