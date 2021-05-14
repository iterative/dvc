from dvc.dependency.base import BaseDependency
from dvc.output.base import BaseOutput

from ..fs.http import HTTPFileSystem


class HTTPDependency(BaseDependency, BaseOutput):
    FS_CLS = HTTPFileSystem
