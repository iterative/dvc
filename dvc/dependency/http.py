from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput
from dvc.remote.http import HTTPRemoteTree


class HTTPDependency(BaseDependency, BaseOutput):
    TREE_CLS = HTTPRemoteTree
