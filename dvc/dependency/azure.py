from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..fs.azure import AzureFileSystem


class AzureDependency(BaseDependency, BaseOutput):
    FS_CLS = AzureFileSystem
