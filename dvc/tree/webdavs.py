from dvc.scheme import Schemes

from .webdav import WebDAVTree


class WebDAVSTree(WebDAVTree):  # pylint:disable=abstract-method
    scheme = Schemes.WEBDAVS
