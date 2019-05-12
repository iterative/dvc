from dvc.scheme import Schemes
from .base import PathCloudBASE


class PathS3(PathCloudBASE):
    scheme = Schemes.S3
