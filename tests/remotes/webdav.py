# pylint:disable=abstract-method
import os
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from wsgiref.simple_server import make_server

import pytest
from funcy import first
from wsgidav.wsgidav_app import WsgiDAVApp

from dvc.path_info import WebDAVURLInfo
from tests.utils.httpd import run_server_on_thread

from .base import Base

AUTH = {"user1": {"password": "password1"}}
_WEBDAV_ROOT = TemporaryDirectory()
_WEBDAV_DIR = Path(_WEBDAV_ROOT.name)


class Webdav(Base, WebDAVURLInfo):
    @staticmethod
    def get_url(port, root_id):  # pylint: disable=arguments-differ
        return f"webdav://localhost:{port}/{root_id}/"

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        self.dir_path.mkdir(parents=parents, exist_ok=True)

    def write_bytes(self, contents):
        self.dir_path.write_bytes(contents)

    def write_text(self, contents, encoding=None, errors=None):
        self.dir_path.write_text(contents, encoding=encoding, errors=errors)

    @property
    def dir_path(self):
        return _WEBDAV_DIR / self.path[1:]


@pytest.fixture
def webdav_server(test_config, tmp_path_factory):
    test_config.requires("webdav")

    host, port = "localhost", 0
    root_id = str(uuid.uuid4())
    root_dir = _WEBDAV_DIR / root_id
    root_dir.mkdir()
    dirmap = {f"/{root_id}": os.fspath(_WEBDAV_DIR / root_id)}

    app = WsgiDAVApp(
        {
            "host": host,
            "port": port,
            "provider_mapping": dirmap,
            "simple_dc": {"user_mapping": {"*": AUTH}},
        }
    )
    server = make_server(host, port, app)
    server.root_id = root_id
    with run_server_on_thread(server) as httpd:
        yield httpd


@pytest.fixture
def webdav(webdav_server):
    url = Webdav.get_url(webdav_server.server_port, webdav_server.root_id)
    ret = Webdav(url)
    user, secrets = first(AUTH.items())
    ret.config = {"url": url, "user": user, **secrets}
    yield ret
