from .http import DependencyHTTP
from dvc.remote.https import HTTPSRemote


class DependencyHTTPS(DependencyHTTP):
    REMOTE = HTTPSRemote
