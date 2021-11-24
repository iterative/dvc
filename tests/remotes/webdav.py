# pylint:disable=abstract-method
import os
from wsgiref.simple_server import make_server

import pytest
from funcy import cached_property, first
from wsgidav.wsgidav_app import WsgiDAVApp

from dvc.testing.cloud import Cloud
from dvc.testing.path_info import WebDAVURLInfo
from tests.utils.httpd import run_server_on_thread

AUTH = {"user1": {"password": "password1"}}


class Webdav(Cloud, WebDAVURLInfo):
    @staticmethod
    def get_url(port):  # pylint: disable=arguments-differ
        return f"webdav://localhost:{port}"

    @property
    def config(self):
        user, secrets = first(AUTH.items())
        url = f"webdav://{self.netloc}{self._spath}"
        return {"url": url, "user": user, **secrets}

    @cached_property
    def client(self):
        from webdav4.client import Client

        user, secrets = first(AUTH.items())
        return Client(
            self.replace(path="").url, auth=(user, secrets["password"])
        )

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        parent_dirs = list(reversed(self.parents))[1:] if parents else []
        for d in parent_dirs + [self]:
            path = d.path  # pylint: disable=no-member
            if not self.client.exists(path):
                self.client.mkdir(path)

    def write_bytes(self, contents):
        from io import BytesIO

        self.client.upload_fileobj(BytesIO(contents), self.path)

    @property
    def fs_path(self):
        return self.path.lstrip("/")

    def exists(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def is_file(self):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError


@pytest.fixture
def webdav_server(test_config, tmp_path_factory):
    test_config.requires("webdav")

    host, port = "localhost", 0
    directory = os.fspath(tmp_path_factory.mktemp("http"))
    dirmap = {"/": directory}

    app = WsgiDAVApp(
        {
            "host": host,
            "port": port,
            "provider_mapping": dirmap,
            "simple_dc": {"user_mapping": {"*": AUTH}},
        }
    )
    server = make_server(host, port, app)
    with run_server_on_thread(server) as httpd:
        yield httpd


@pytest.fixture
def make_webdav(webdav_server):
    def _make_webdav():
        url = Webdav.get_url(webdav_server.server_port)
        return Webdav(url)

    return _make_webdav


@pytest.fixture
def webdav(make_webdav):
    return make_webdav()
