from dvc.scheme import Schemes

from .http import HTTPRemoteTree


class HTTPSRemoteTree(HTTPRemoteTree):
    scheme = Schemes.HTTPS
