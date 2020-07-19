from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..tree.azure import AzureTree


class AzureDependency(BaseDependency, BaseOutput):
    TREE_CLS = AzureTree
