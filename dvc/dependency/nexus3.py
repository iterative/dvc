from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..fs.nexus3 import Nexus3FileSystem


class Nexus3Dependency(BaseDependency, BaseOutput):
    FS_CLS = Nexus3FileSystem
