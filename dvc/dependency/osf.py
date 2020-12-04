from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..tree.osf import OSFTree


class OSFDependency(BaseDependency, BaseOutput):
    TREE_CLS = OSFTree
