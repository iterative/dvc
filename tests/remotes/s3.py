import locale
import os
import time
import uuid

import pytest
from funcy import cached_property

from dvc.path_info import CloudURLInfo
from dvc.utils import env2bool

from .base import Base

TEST_AWS_REPO_BUCKET = os.environ.get("DVC_TEST_AWS_REPO_BUCKET", "dvc-temp")

TEST_AWS_S3_PORT = 5555
TEST_AWS_ENDPOINT_URL = f"http://127.0.0.1:{TEST_AWS_S3_PORT}/"


class S3(Base, CloudURLInfo):

    IS_OBJECT_STORAGE = True

    @cached_property
    def config(self):
        return {"url": self.url, "endpointurl": TEST_AWS_ENDPOINT_URL}

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

        return boto3.client("s3", endpoint_url=TEST_AWS_ENDPOINT_URL)

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

    def write_bytes(self, contents):
        self._s3.put_object(
            Bucket=self.bucket, Key=self.path, Body=contents,
        )

    def read_bytes(self):
        data = self._s3.get_object(Bucket=self.bucket, Key=self.path)
        return data["Body"].read()

    def read_text(self, encoding=None, errors=None):
        if not encoding:
            encoding = locale.getpreferredencoding(False)
        assert errors is None
        return self.read_bytes().decode(encoding)


@pytest.fixture(scope="session")
def s3_fake_creds_file(monkeypatch):
    # https://github.com/spulec/moto#other-caveats
    import pathlib

    aws_dir = pathlib.Path("~").expanduser() / ".aws"
    aws_dir.mkdir(exist_ok=True)

    aws_creds = aws_dir / "credentials"
    exists = aws_creds.exists()

    if not exists:
        aws_creds.touch()

    with monkeypatch.context() as m:
        m.setenv("AWS_ACCESS_KEY_ID", "testing")
        m.setenv("AWS_SECRET_ACCESS_KEY", "testing")
        m.setenv("AWS_SECURITY_TOKEN", "testing")
        m.setenv("AWS_SESSION_TOKEN", "testing")
        yield

    if not exists:
        aws_creds.unlink()


# Due to moto being uncompatible with aioboto on wrapper
# mode, we start the moto server as a subprocess (imitates
# a real S3 service) and then create a client that uses
# this server.
#
# Originally adopted from:
# https://github.com/dask/s3fs/blob/main/s3fs/tests/test_s3fs.py#L66-L86
@pytest.fixture(scope="session")
def s3_server(test_config):
    test_config.requires("s3")

    import shlex
    import subprocess

    import requests

    proc = subprocess.Popen(
        shlex.split(f"moto_server s3 -p {TEST_AWS_S3_PORT}")
    )

    timeout = 5
    while timeout > 0:
        try:
            r = requests.get(TEST_AWS_ENDPOINT_URL)
            if r.ok:
                break
        except requests.RequestException:
            pass
        timeout -= 0.1
        time.sleep(0.1)

    yield

    proc.terminate()
    proc.wait()


@pytest.fixture
def s3(s3_server, s3_fake_creds_file):
    workspace = S3(S3.get_url())
    workspace._s3.create_bucket(Bucket=TEST_AWS_REPO_BUCKET)
    yield workspace


@pytest.fixture
def real_s3(test_config):
    test_config.requires("s3")
    if not S3.should_test():
        pytest.skip("no real s3")
    yield S3(S3.get_url())
