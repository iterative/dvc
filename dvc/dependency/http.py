from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..tree.http import HTTPTree


class HTTPDependency(BaseDependency, BaseOutput):
    TREE_CLS = HTTPTree
