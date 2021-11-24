# pylint:disable=abstract-method
import os
import uuid

import pytest
import requests

from dvc.testing.cloud import Cloud
from dvc.testing.path_info import HTTPURLInfo


class HTTP(Cloud, HTTPURLInfo):
    @staticmethod
    def get_url(port):  # pylint: disable=arguments-differ
        dname = str(uuid.uuid4())
        return f"http://127.0.0.1:{port}/{dname}"

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents

    def write_bytes(self, contents):
        assert isinstance(contents, bytes)
        response = requests.post(self.url, data=contents)
        assert response.status_code == 200

    @property
    def config(self):
        return {"url": self.url}

    @property
    def fs_path(self):
        return self.url

    def exists(self):
        raise NotImplementedError

    def is_dir(self):
        raise NotImplementedError

    def is_file(self):
        raise NotImplementedError

    def read_bytes(self):
        raise NotImplementedError


@pytest.fixture(scope="session")
def http_server(test_config, tmp_path_factory):
    from tests.utils.httpd import StaticFileServer

    test_config.requires("http")
    directory = os.fspath(tmp_path_factory.mktemp("http"))
    with StaticFileServer(directory=directory) as httpd:
        yield httpd


@pytest.fixture
def make_http(http_server):
    def _make_http():
        return HTTP(HTTP.get_url(http_server.server_port))

    return _make_http


@pytest.fixture
def http(make_http):
    return make_http()
