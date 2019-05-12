from dvc.scheme import Schemes
from .base import PathCloudBASE


class PathAZURE(PathCloudBASE):
    scheme = Schemes.AZURE
