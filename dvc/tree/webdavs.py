from dvc.scheme import Schemes

from .webdav import WebdavTree


class WebdavsTree(WebdavTree):  # pylint:disable=abstract-method
    scheme = Schemes.WEBDAVS
