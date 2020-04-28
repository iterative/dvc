from dvc.dependency.base import BaseDependency
from dvc.output.s3 import OutputS3


class S3Dependency(BaseDependency, OutputS3):
    pass
