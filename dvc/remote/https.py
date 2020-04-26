from .http import HTTPRemote
from dvc.scheme import Schemes


class HTTPSRemote(HTTPRemote):
    scheme = Schemes.HTTPS
