from dvc.output.base import BaseOutput

from ..fs.nexus3 import Nexus3FileSystem


class Nexus3Output(BaseOutput):
    FS_CLS = Nexus3FileSystem
