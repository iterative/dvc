from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..tree.webdav import WebDAVTree


class WebDAVDependency(BaseDependency, BaseOutput):
    TREE_CLS = WebDAVTree
