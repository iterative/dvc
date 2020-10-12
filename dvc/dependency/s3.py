from dvc.dependency.base import BaseDependency
from dvc.output.s3 import S3Output


class S3Dependency(BaseDependency, S3Output):
    pass
