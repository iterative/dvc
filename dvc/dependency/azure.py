from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput
from dvc.remote.azure import AzureRemote


class AzureDependency(BaseDependency, BaseOutput):
    REMOTE = AzureRemote
