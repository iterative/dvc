from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..fs.gs import GSFileSystem


class GSDependency(BaseDependency, BaseOutput):
    FS_CLS = GSFileSystem
