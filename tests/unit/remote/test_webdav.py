import pytest

from dvc.exceptions import HTTPError
from dvc.path_info import WebdavURLInfo
from dvc.remote.webdav import RemoteWEBDAV
from tests.utils.httpd import StaticFileServer, WebDavSimpleHandler


def test_create_collections(dvc):
    with StaticFileServer(handler_class=WebDavSimpleHandler) as httpd:
        url0 = "webdav://localhost:{}/a/b/file.txt".format(httpd.server_port)
        url1 = "webdav://localhost:{}/a/c/file.txt".format(httpd.server_port)
        config = {"url": url0}

        remote = RemoteWEBDAV(dvc, config)

        remote._create_collections(WebdavURLInfo(url0))

        with pytest.raises(HTTPError):
            remote._create_collections(WebdavURLInfo(url1))
