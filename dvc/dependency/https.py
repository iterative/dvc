from ..fs.https import HTTPSFileSystem
from .http import HTTPDependency


class HTTPSDependency(HTTPDependency):
    FS_CLS = HTTPSFileSystem
