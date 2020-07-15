from dvc.scheme import Schemes

from .http import HTTPRemoteTree


class HTTPSRemoteTree(HTTPRemoteTree):  # pylint:disable=abstract-method
    scheme = Schemes.HTTPS
