import boto3
import os
import pytest
from moto import mock_s3

from dvc.remote.s3 import RemoteS3


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3(aws_credentials):
    """Returns a connection to a bucket with the following file structure:

        bucket
        ├── data
        │  ├── alice
        │  ├── alpha
        │  └── subdir
        │     ├── 1
        │     ├── 2
        │     └── 3
        ├── empty_dir
        ├── empty_file
        └── foo
    """
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="bucket")

        s3.put_object(Bucket="bucket", Key="empty_dir/")
        s3.put_object(Bucket="bucket", Key="empty_file", Body=b"")
        s3.put_object(Bucket="bucket", Key="foo", Body=b"foo")
        s3.put_object(Bucket="bucket", Key="data/alice", Body=b"alice")
        s3.put_object(Bucket="bucket", Key="data/alpha", Body=b"alpha")
        s3.put_object(Bucket="bucket", Key="data/subdir/1", Body=b"1")
        s3.put_object(Bucket="bucket", Key="data/subdir/2", Body=b"2")
        s3.put_object(Bucket="bucket", Key="data/subdir/3", Body=b"3")

        yield s3


@pytest.fixture
def remote():
    """Returns RemoteS3 instance to work with the `s3` fixture."""
    yield RemoteS3(None, {"url": "s3://bucket", "region": "us-east-1"})


def test_isdir(s3, remote):
    assert remote.isdir(remote.path_info / "data")
    assert remote.isdir(remote.path_info / "data/")
    assert remote.isdir(remote.path_info / "data/subdir")
    assert remote.isdir(remote.path_info / "empty_dir")
    assert not remote.isdir(remote.path_info / "foo")
    assert not remote.isdir(remote.path_info / "data/alice")
    assert not remote.isdir(remote.path_info / "data/al")
    assert not remote.isdir(remote.path_info / "data/subdir/1")


def test_exists(s3, remote):
    assert remote.exists(remote.path_info / "data")
    assert remote.exists(remote.path_info / "data/")
    assert remote.exists(remote.path_info / "data/subdir")
    assert remote.exists(remote.path_info / "empty_dir")
    assert remote.exists(remote.path_info / "empty_file")
    assert remote.exists(remote.path_info / "foo")
    assert remote.exists(remote.path_info / "data/alice")
    assert remote.exists(remote.path_info / "data/subdir/1")
    assert not remote.exists(remote.path_info / "data/al")


def test_walk_files(s3, remote):
    files = [
        remote.path_info / "data/alice",
        remote.path_info / "data/alpha",
        remote.path_info / "data/subdir/1",
        remote.path_info / "data/subdir/2",
        remote.path_info / "data/subdir/3",
    ]

    assert list(remote.walk_files(remote.path_info / "data")) == files
