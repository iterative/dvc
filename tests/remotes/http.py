import pytest

from .base import Base


class HTTP(Base):
    @staticmethod
    def get_url(port):
        return f"http://127.0.0.1:{port}"


@pytest.fixture
def http_server(tmp_dir):
    from tests.utils.httpd import PushRequestHandler, StaticFileServer

    with StaticFileServer(handler_class=PushRequestHandler) as httpd:
        yield httpd


@pytest.fixture
def http(http_server):
    yield {"url": HTTP.get_url(http_server.server_port)}


@pytest.fixture
def http_remote(tmp_dir, dvc, http):
    tmp_dir.add_remote(config=http)
    yield http
