from .http import HTTPDependency
from dvc.remote.https import HTTPSRemote


class HTTPSDependency(HTTPDependency):
    REMOTE = HTTPSRemote
