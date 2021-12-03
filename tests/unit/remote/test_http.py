import ssl

from mock import patch

from dvc.fs.http import HTTPFileSystem


def test_public_auth_method(dvc):
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "user": "",
        "password": "",
    }

    fs = HTTPFileSystem(**config)

    assert "auth" not in fs.fs_args["client_kwargs"]
    assert "headers" not in fs.fs_args


def test_basic_auth_method(dvc):
    user = "username"
    password = "password"
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "auth": "basic",
        "user": user,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    assert fs.fs_args["client_kwargs"]["auth"].login == user
    assert fs.fs_args["client_kwargs"]["auth"].password == password


def test_custom_auth_method(dvc):
    header = "Custom-Header"
    password = "password"
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "auth": "custom",
        "custom_auth_header": header,
        "password": password,
    }

    fs = HTTPFileSystem(**config)

    headers = fs.fs_args["headers"]
    assert header in headers
    assert headers[header] == password


def test_ssl_verify_disable(dvc):
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "ssl_verify": False,
    }

    fs = HTTPFileSystem(**config)
    assert not fs.fs_args["client_kwargs"]["connector"]._ssl


@patch("ssl.SSLContext.load_verify_locations")
def test_ssl_verify_custom_cert(dvc, mocker):
    config = {
        "url": "http://example.com/",
        "path": "file.html",
        "ssl_verify": "/path/to/custom/cabundle.pem",
    }

    fs = HTTPFileSystem(**config)

    assert isinstance(
        fs.fs_args["client_kwargs"]["connector"]._ssl, ssl.SSLContext
    )


def test_http_method(dvc):
    config = {
        "url": "http://example.com/",
        "path": "file.html",
    }

    fs = HTTPFileSystem(**config, method="PUT")
    assert fs.upload_method == "PUT"

    fs = HTTPFileSystem(**config, method="POST")
    assert fs.upload_method == "POST"
