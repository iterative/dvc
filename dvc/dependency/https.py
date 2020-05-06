from dvc.remote.https import HTTPSRemote

from .http import HTTPDependency


class HTTPSDependency(HTTPDependency):
    REMOTE = HTTPSRemote
