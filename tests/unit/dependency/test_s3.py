from dvc.dependency.s3 import S3Dependency
from tests.unit.dependency.test_local import TestLocalDependency


class TestS3Dependency(TestLocalDependency):
    def _get_cls(self):
        return S3Dependency
