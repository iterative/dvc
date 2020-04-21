import pytest

from dvc.exceptions import HTTPError
from dvc.path_info import WebdavURLInfo
from dvc.remote.webdav import RemoteWEBDAV
from tests.utils.httpd import StaticFileServer, WebDavSimpleHandler


def test_create_collections(dvc):
    with StaticFileServer(handler_class=WebDavSimpleHandler) as httpd:
        url = "webdav://localhost:{}/a/b/file.txt".format(httpd.server_port)
        config = {"url": url}

        remote = RemoteWEBDAV(dvc, config)

        remote._create_collections(WebdavURLInfo(url))

        with pytest.raises(HTTPError):
            remote._create_collections(WebdavURLInfo(url + "/check"))
