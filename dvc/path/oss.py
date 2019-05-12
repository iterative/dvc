from dvc.scheme import Schemes
from .base import PathCloudBASE


class PathOSS(PathCloudBASE):
    scheme = Schemes.OSS
