from dvc.scheme import Schemes

from .http import HTTPRemote


class HTTPSRemote(HTTPRemote):
    scheme = Schemes.HTTPS
