import locale
import os
import uuid

import pytest
from funcy import cached_property
from moto import mock_s3

from dvc.path_info import CloudURLInfo
from dvc.utils import env2bool

from .base import Base

TEST_AWS_REPO_BUCKET = os.environ.get("DVC_TEST_AWS_REPO_BUCKET", "dvc-temp")


class S3(Base, CloudURLInfo):
    @staticmethod
    def should_test():
        do_test = env2bool("DVC_TEST_AWS", undefined=None)
        if do_test is not None:
            return do_test

        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        ):
            return True

        return False

    @staticmethod
    def _get_storagepath():
        return (
            TEST_AWS_REPO_BUCKET
            + "/"
            + "dvc_test_caches"
            + "/"
            + str(uuid.uuid4())
        )

    @staticmethod
    def get_url():
        return "s3://" + S3._get_storagepath()

    @cached_property
    def _s3(self):
        import boto3

        return boto3.client("s3")

    def is_file(self):
        from botocore.exceptions import ClientError

        if self.path.endswith("/"):
            return False

        try:
            self._s3.head_object(Bucket=self.bucket, Key=self.path)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "404":
                raise
            return False

        return True

    def is_dir(self):
        path = (self / "").path
        resp = self._s3.list_objects(Bucket=self.bucket, Prefix=path)
        return bool(resp.get("Contents"))

    def exists(self):
        return self.is_file() or self.is_dir()

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        assert mode == 0o777
        assert parents
        assert not exist_ok

    def write_bytes(self, contents):
        self._s3.put_object(
            Bucket=self.bucket, Key=self.path, Body=contents,
        )

    def write_text(self, contents, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        self.write_bytes(contents.encode(encoding))

    def read_bytes(self):
        data = self._s3.get_object(Bucket=self.bucket, Key=self.path)
        return data["Body"].read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture
def s3():
    with mock_s3():
        import boto3

        boto3.client("s3").create_bucket(Bucket=TEST_AWS_REPO_BUCKET)

        yield S3(S3.get_url())


@pytest.fixture
def real_s3():
    if not S3.should_test():
        pytest.skip("no real s3")
    yield S3(S3.get_url())
