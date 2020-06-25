import os

import pytest

from dvc.path_info import PathInfo
from dvc.remote.s3 import S3RemoteTree
from dvc.utils.fs import walk_files
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
def remote(request, dvc):
    if not request.param.should_test():
        raise pytest.skip()
    with request.param.remote(dvc) as _remote:
        request.param.put_objects(_remote, FILE_WITH_CONTENTS)
        yield _remote


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
        assert remote.tree.isdir(remote.path_info / path) == expected


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
        assert remote.tree.exists(remote.path_info / path) == expected


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

    assert list(remote.tree.walk_files(remote.path_info / "data")) == files


@pytest.mark.parametrize("remote", [S3Mocked], indirect=True)
def test_copy_preserve_etag_across_buckets(remote, dvc):
    s3 = remote.tree.s3
    s3.create_bucket(Bucket="another")

    another = S3RemoteTree(dvc, {"url": "s3://another", "region": "us-east-1"})

    from_info = remote.path_info / "foo"
    to_info = another.path_info / "foo"

    remote.tree.copy(from_info, to_info)

    from_etag = S3RemoteTree.get_etag(s3, from_info.bucket, from_info.path)
    to_etag = S3RemoteTree.get_etag(s3, "another", "foo")

    assert from_etag == to_etag


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_makedirs(remote):
    tree = remote.tree
    empty_dir = remote.path_info / "empty_dir" / ""
    tree.remove(empty_dir)
    assert not tree.exists(empty_dir)
    tree.makedirs(empty_dir)
    assert tree.exists(empty_dir)
    assert tree.isdir(empty_dir)


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
        assert remote.tree.isfile(remote.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_download_dir(remote, tmpdir):
    path = str(tmpdir / "data")
    to_info = PathInfo(path)
    remote.tree.download(remote.path_info / "data", to_info)
    assert os.path.isdir(path)
    data_dir = tmpdir / "data"
    assert len(list(walk_files(path))) == 7
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
