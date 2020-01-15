import pytest

from dvc.config import ConfigError
from dvc.exceptions import HTTPError
from dvc.path_info import URLInfo
from dvc.remote.http import RemoteHTTP
from tests.utils.httpd import StaticFileServer


def test_no_traverse_compatibility(dvc):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "no_traverse": False,
    }

    with pytest.raises(ConfigError):
        RemoteHTTP(dvc, config)


def test_download_fails_on_error_code(dvc):
    with StaticFileServer() as httpd:
        url = "http://localhost:{}/".format(httpd.server_port)
        config = {"url": url}

        remote = RemoteHTTP(dvc, config)

        with pytest.raises(HTTPError):
            remote._download(URLInfo(url) / "missing.txt", "missing.txt")
