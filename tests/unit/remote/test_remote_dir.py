# -*- coding: utf-8 -*-
import pytest
import os
from dvc.remote.s3 import RemoteS3
from dvc.utils import walk_files
from dvc.path_info import PathInfo
from tests.remotes import GCP, S3Mocked

remotes = [GCP, S3Mocked]

FILE_WITH_CONTENTS = {
    "data1.txt": "",
    "empty_dir/": "",
    "empty_file": "",
    "foo": "foo",
    "data/alice": "alice",
    "data/alpha": "alpha",
    "data/subdir-file.txt": "subdir",
    "data/subdir/1": "1",
    "data/subdir/2": "2",
    "data/subdir/3": "3",
    "data/subdir/empty_dir/": "",
    "data/subdir/empty_file": "",
}


@pytest.fixture
def remote(request):
    if not request.param.should_test():
        raise pytest.skip()
    with request.param.remote() as remote:
        request.param.put_objects(remote, FILE_WITH_CONTENTS)
        yield remote


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_isdir(remote):
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


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_exists(remote):
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
        (True, "data1.txt"),
    ]

    for expected, path in test_cases:
        assert remote.exists(remote.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_walk_files(remote):
    files = [
        remote.path_info / "data/alice",
        remote.path_info / "data/alpha",
        remote.path_info / "data/subdir-file.txt",
        remote.path_info / "data/subdir/1",
        remote.path_info / "data/subdir/2",
        remote.path_info / "data/subdir/3",
        remote.path_info / "data/subdir/empty_file",
    ]

    assert list(remote.walk_files(remote.path_info / "data")) == files


@pytest.mark.parametrize("remote", [S3Mocked], indirect=True)
def test_copy_preserve_etag_across_buckets(remote):
    s3 = remote.s3
    s3.create_bucket(Bucket="another")

    another = RemoteS3(None, {"url": "s3://another", "region": "us-east-1"})

    from_info = remote.path_info / "foo"
    to_info = another.path_info / "foo"

    remote.copy(from_info, to_info)

    from_etag = RemoteS3.get_etag(s3, from_info.bucket, from_info.path)
    to_etag = RemoteS3.get_etag(s3, "another", "foo")

    assert from_etag == to_etag


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_makedirs(remote):
    empty_dir = remote.path_info / "empty_dir" / ""
    remote.remove(empty_dir)
    assert not remote.exists(empty_dir)
    remote.makedirs(empty_dir)
    assert remote.exists(empty_dir)
    assert remote.isdir(empty_dir)


@pytest.mark.parametrize("remote", [GCP, S3Mocked], indirect=True)
def test_isfile(remote):
    test_cases = [
        (False, "empty_dir/"),
        (True, "empty_file"),
        (True, "foo"),
        (True, "data/alice"),
        (True, "data/alpha"),
        (True, "data/subdir/1"),
        (True, "data/subdir/2"),
        (True, "data/subdir/3"),
        (False, "data/subdir/empty_dir/"),
        (True, "data/subdir/empty_file"),
        (False, "something-that-does-not-exist"),
        (False, "data/subdir/empty-file/"),
        (False, "empty_dir"),
    ]

    for expected, path in test_cases:
        assert remote.isfile(remote.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_download_dir(remote, tmpdir):
    path = str(tmpdir / "data")
    to_info = PathInfo(path)
    remote.download(remote.path_info / "data", to_info)
    assert os.path.isdir(path)
    data_dir = tmpdir / "data"
    assert len(list(walk_files(path, None))) == 7
    assert (data_dir / "alice").read_text(encoding="utf-8") == "alice"
    assert (data_dir / "alpha").read_text(encoding="utf-8") == "alpha"
    assert (data_dir / "subdir-file.txt").read_text(
        encoding="utf-8"
    ) == "subdir"
    assert (data_dir / "subdir" / "1").read_text(encoding="utf-8") == "1"
    assert (data_dir / "subdir" / "2").read_text(encoding="utf-8") == "2"
    assert (data_dir / "subdir" / "3").read_text(encoding="utf-8") == "3"
    assert (data_dir / "subdir" / "empty_file").read_text(
        encoding="utf-8"
    ) == ""
