import os

import pytest

from dvc.path_info import PathInfo
from dvc.remote import get_remote
from dvc.tree.s3 import S3Tree
from dvc.utils.fs import walk_files

remotes = [pytest.lazy_fixture(fix) for fix in ["gs", "s3"]]

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
    cloud = request.param
    cloud.gen(FILE_WITH_CONTENTS)
    return get_remote(dvc, **cloud.config)


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
        assert remote.tree.isdir(remote.tree.path_info / path) == expected


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
        assert remote.tree.exists(remote.tree.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_walk_files(remote):
    files = [
        remote.tree.path_info / "data/alice",
        remote.tree.path_info / "data/alpha",
        remote.tree.path_info / "data/subdir-file.txt",
        remote.tree.path_info / "data/subdir/1",
        remote.tree.path_info / "data/subdir/2",
        remote.tree.path_info / "data/subdir/3",
        remote.tree.path_info / "data/subdir/empty_file",
    ]

    assert (
        list(remote.tree.walk_files(remote.tree.path_info / "data")) == files
    )


@pytest.mark.parametrize("remote", [pytest.lazy_fixture("s3")], indirect=True)
def test_copy_preserve_etag_across_buckets(remote, dvc):
    s3 = remote.tree.s3
    s3.Bucket("another").create()

    another = S3Tree(dvc, {"url": "s3://another", "region": "us-east-1"})

    from_info = remote.tree.path_info / "foo"
    to_info = another.path_info / "foo"

    remote.tree.copy(from_info, to_info)

    from_hash = remote.tree.get_hash(from_info)
    to_hash = another.get_hash(to_info)

    assert from_hash == to_hash


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_makedirs(remote):
    tree = remote.tree
    empty_dir = remote.tree.path_info / "empty_dir" / ""
    tree.remove(empty_dir)
    assert not tree.exists(empty_dir)
    tree.makedirs(empty_dir)
    assert tree.exists(empty_dir)
    assert tree.isdir(empty_dir)


@pytest.mark.parametrize("remote", remotes, indirect=True)
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
        assert remote.tree.isfile(remote.tree.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_download_dir(remote, tmpdir):
    path = str(tmpdir / "data")
    to_info = PathInfo(path)
    remote.tree.download(remote.tree.path_info / "data", to_info)
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
