from dvc.output.s3 import OutputS3

from tests.unit.output.test_local import TestOutputLOCAL


class TestOutputS3(TestOutputLOCAL):
    def _get_cls(self):
        return OutputS3
