from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput
from dvc.remote.azure import AzureRemoteTree


class AzureDependency(BaseDependency, BaseOutput):
    TREE_CLS = AzureRemoteTree
