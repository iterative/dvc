# -*- coding: utf-8 -*-
import pytest
import uuid

from moto import mock_s3

from dvc.remote.gs import RemoteGS
from dvc.remote.s3 import RemoteS3

from tests.func.test_data_cloud import _should_test_gcp

test_gs = pytest.mark.skipif(not _should_test_gcp(), reason="Skipping on gs.")


def create_object_gs(client, bucket, key, body):
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(key)
    blob.upload_from_string(body)


@pytest.fixture
def remote_s3():
    """Returns a RemoteS3 connected to a bucket with the following structure:

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
        remote = RemoteS3(None, {"url": "s3://bucket", "region": "us-east-1"})
        s3 = remote.s3

        s3.create_bucket(Bucket="bucket")
        s3.put_object(Bucket="bucket", Key="empty_dir/")
        s3.put_object(Bucket="bucket", Key="empty_file", Body=b"")
        s3.put_object(Bucket="bucket", Key="foo", Body=b"foo")
        s3.put_object(Bucket="bucket", Key="data/alice", Body=b"alice")
        s3.put_object(Bucket="bucket", Key="data/alpha", Body=b"alpha")
        s3.put_object(Bucket="bucket", Key="data/subdir/1", Body=b"1")
        s3.put_object(Bucket="bucket", Key="data/subdir/2", Body=b"2")
        s3.put_object(Bucket="bucket", Key="data/subdir/3", Body=b"3")

        yield remote


@pytest.fixture
def remote_gs():
    """Returns a RemoteGS connected to a bucket with the following structure:
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
    prefix = str(uuid.uuid4())
    REMOTE_URL = "gs://dvc-test/" + prefix
    remote = RemoteGS(None, {"url": REMOTE_URL})
    teardowns = []

    def put_object(file, content):
        create_object_gs(remote.gs, "dvc-test", prefix + "/" + file, content)
        teardowns.append(lambda: remote.remove(remote.path_info / file))

    put_object("empty_dir/", "")
    put_object("empty_file", "")
    put_object("foo", "foo")
    put_object("data/alice", "alice")
    put_object("data/alpha", "alpha")
    put_object("data/subdir/1", "1")
    put_object("data/subdir/2", "2")
    put_object("data/subdir/3", "3")
    put_object("data/subdir/4/", "")

    yield remote

    for teardown in teardowns:
        teardown()


remote_parameterized = pytest.mark.parametrize(
    "remote_name", [pytest.param("remote_gs", marks=test_gs), "remote_s3"]
)


@remote_parameterized
def test_isdir(request, remote_name):
    remote = request.getfixturevalue(remote_name)

    test_cases = [
        (True, "data"),
        (True, "data/"),
        (True, "data/subdir"),
        (True, "empty_dir"),
        (False, "foo"),
        (False, "data/alice"),
        (False, "data/al"),
        (False, "data/subdir/1"),
    ]

    for expected, path in test_cases:
        assert remote.isdir(remote.path_info / path) == expected


@remote_parameterized
def test_exists(request, remote_name):
    remote = request.getfixturevalue(remote_name)

    test_cases = [
        (True, "data"),
        (True, "data/"),
        (True, "data/subdir"),
        (True, "empty_dir"),
        (True, "empty_file"),
        (True, "foo"),
        (True, "data/alice"),
        (True, "data/subdir/1"),
        (False, "data/al"),
        (False, "foo/"),
    ]

    for expected, path in test_cases:
        assert remote.exists(remote.path_info / path) == expected


@remote_parameterized
def test_walk_files(request, remote_name):
    remote = request.getfixturevalue(remote_name)

    files = [
        remote.path_info / "data/alice",
        remote.path_info / "data/alpha",
        remote.path_info / "data/subdir/1",
        remote.path_info / "data/subdir/2",
        remote.path_info / "data/subdir/3",
    ]

    assert list(remote.walk_files(remote.path_info / "data")) == files


def test_copy_preserve_etag_across_buckets(remote_s3):
    s3 = remote_s3.s3
    s3.create_bucket(Bucket="another")

    another = RemoteS3(None, {"url": "s3://another", "region": "us-east-1"})

    from_info = remote_s3.path_info / "foo"
    to_info = another.path_info / "foo"

    remote_s3.copy(from_info, to_info)

    from_etag = RemoteS3.get_etag(s3, "bucket", "foo")
    to_etag = RemoteS3.get_etag(s3, "another", "foo")

    assert from_etag == to_etag
