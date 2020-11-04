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


def test_ssl_verify_is_enabled_by_default(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
    }

    tree = HTTPTree(dvc, config)

    assert tree._session.verify is True


def test_ssl_verify_disable(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "ssl_verify": False,
    }

    tree = HTTPTree(dvc, config)

    assert tree._session.verify is False


def test_http_method(dvc):
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
        "method": "PUT",
    }

    tree = HTTPTree(dvc, config)

    assert tree._auth_method() == auth
    assert tree.method == "PUT"
    assert isinstance(tree._auth_method(), HTTPBasicAuth)


def test_exists(mocker):
    import io

    import requests

    from dvc.path_info import URLInfo

    res = requests.Response()
    # need to add `raw`, as `exists()` fallbacks to a streaming GET requests
    # on HEAD request failure.
    res.raw = io.StringIO("foo")

    tree = HTTPTree(None, {})
    mocker.patch.object(tree, "request", return_value=res)

    url = URLInfo("https://example.org/file.txt")

    res.status_code = 200
    assert tree.exists(url) is True

    res.status_code = 404
    assert tree.exists(url) is False

    res.status_code = 403
    with pytest.raises(HTTPError):
        tree.exists(url)
