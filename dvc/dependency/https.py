from .http import DependencyHTTP
from dvc.remote.https import RemoteHTTPS


class DependencyHTTPS(DependencyHTTP):
    REMOTE = RemoteHTTPS
