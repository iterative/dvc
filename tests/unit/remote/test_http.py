import pytest

from dvc.exceptions import HTTPError
from dvc.tree.http import HTTPTree


def test_download_fails_on_error_code(dvc, http):
    tree = HTTPTree(dvc, http.config)

    with pytest.raises(HTTPError):
        tree._download(http / "missing.txt", "missing.txt")


def test_public_auth_method(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "user": "",
        "password": "",
    }

    tree = HTTPTree(dvc, config)

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

    tree = HTTPTree(dvc, config)

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

    tree = HTTPTree(dvc, config)

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

    tree = HTTPTree(dvc, config)

    assert tree._auth_method() is None
    assert header in tree.headers
    assert tree.headers[header] == password
