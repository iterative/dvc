from dvc.dependency.base import BaseDependency
from dvc.output.azure import AzureOutput


class AzureDependency(BaseDependency, AzureOutput):
    pass
