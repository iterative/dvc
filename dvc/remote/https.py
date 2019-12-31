from .http import RemoteHTTP
from dvc.scheme import Schemes


class RemoteHTTPS(RemoteHTTP):
    scheme = Schemes.HTTPS
