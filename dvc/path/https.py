from dvc.scheme import Schemes
from .http import PathHTTP


class PathHTTPS(PathHTTP):
    scheme = Schemes.HTTPS
