import pytest

from .base import Base


class HTTP(Base):
    @staticmethod
    def get_url(port):  # pylint: disable=arguments-differ
        return f"http://127.0.0.1:{port}"

    def __init__(self, server):
        self.url = self.get_url(server.server_port)


@pytest.fixture
def http_server(tmp_dir):
    from tests.utils.httpd import PushRequestHandler, StaticFileServer

    with StaticFileServer(handler_class=PushRequestHandler) as httpd:
        yield httpd


@pytest.fixture
def http(http_server):
    yield HTTP(http_server)


@pytest.fixture
def http_remote(tmp_dir, dvc, http):
    tmp_dir.add_remote(config=http.config)
    yield http
