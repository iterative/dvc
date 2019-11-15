try:
    from http.server import BaseHTTPRequestHandler
except ImportError:
    from BaseHTTPServer import BaseHTTPRequestHandler

import pytest

from dvc.config import ConfigError
from dvc.exceptions import HTTPError
from dvc.path_info import URLInfo
from dvc.remote.http import RemoteHTTP
from tests.utils.httpd import StaticFileServer


def test_no_traverse_compatibility(dvc_repo):
    config = {
        "url": "http://example.com/",
        "path_info": "file.html",
        "no_traverse": False,
    }

    with pytest.raises(ConfigError):
        RemoteHTTP(dvc_repo, config)


def test_download_fails_on_error_code(dvc_repo):
    class ErrorStatusRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(404, message="Not found")
            self.end_headers()

    with StaticFileServer(ErrorStatusRequestHandler) as httpd:
        url = "http://localhost:{}/".format(httpd.server_port)
        config = {"url": url}

        remote = RemoteHTTP(dvc_repo, config)

        with pytest.raises(HTTPError):
            remote._download(URLInfo(url) / "file.txt", "file.txt")
