from . import Schemes
from .http import HTTPPathInfo


class HTTPSPathInfo(HTTPPathInfo):
    scheme = Schemes.HTTPS
