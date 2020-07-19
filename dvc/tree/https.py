from dvc.scheme import Schemes

from .http import HTTPTree


class HTTPSTree(HTTPTree):  # pylint:disable=abstract-method
    scheme = Schemes.HTTPS
