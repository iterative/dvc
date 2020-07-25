from ..tree.https import HTTPSTree
from .http import HTTPDependency


class HTTPSDependency(HTTPDependency):
    TREE_CLS = HTTPSTree
