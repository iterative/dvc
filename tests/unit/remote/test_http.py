import pytest
from BaseHTTPServer import BaseHTTPRequestHandler

from dvc.config import ConfigError
from dvc.exceptions import HTTPErrorStatusCodeException
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


@pytest.mark.parametrize("response_code", [404, 403, 500])
def test_download_fails_on_error_code(response_code, dvc_repo):
    class ErrorStatusRequestHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(response_code, message="Error message")
            self.end_headers()

    with StaticFileServer(ErrorStatusRequestHandler) as httpd:
        url = "http://localhost:{}/".format(httpd.server_port)
        config = {"url": url}

        remote = RemoteHTTP(dvc_repo, config)
        import os

        with pytest.raises(HTTPErrorStatusCodeException):
            remote._download(
                URLInfo(os.path.join(url, "file.txt")), "file.txt"
            )
