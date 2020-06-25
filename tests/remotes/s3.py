import os
import uuid
from contextlib import contextmanager

import pytest
from moto.s3 import mock_s3

from dvc.remote.base import Remote
from dvc.remote.s3 import S3RemoteTree
from dvc.utils import env2bool

from .base import Base

TEST_AWS_REPO_BUCKET = os.environ.get("DVC_TEST_AWS_REPO_BUCKET", "dvc-temp")


class S3(Base):
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
        return TEST_AWS_REPO_BUCKET + "/" + str(uuid.uuid4())

    @staticmethod
    def get_url():
        return "s3://" + S3._get_storagepath()


@pytest.fixture
def s3():
    if not S3.should_test():
        pytest.skip("no s3")
    yield S3()


@pytest.fixture
def s3_remote(tmp_dir, dvc, s3):
    tmp_dir.add_remote(config=s3.config)
    yield s3


class S3Mocked(S3):
    @classmethod
    @contextmanager
    def remote(cls, repo):
        with mock_s3():
            yield Remote(S3RemoteTree(repo, {"url": cls.get_url()}))

    @staticmethod
    def put_objects(remote, objects):
        client = remote.tree.s3
        bucket = remote.path_info.bucket
        client.create_bucket(Bucket=bucket)
        for key, body in objects.items():
            client.put_object(
                Bucket=bucket, Key=(remote.path_info / key).path, Body=body
            )
