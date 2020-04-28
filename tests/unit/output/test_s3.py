from dvc.output.s3 import S3Output
from tests.unit.output.test_local import TestLocalOutput


class TestS3Output(TestLocalOutput):
    def _get_cls(self):
        return S3Output
