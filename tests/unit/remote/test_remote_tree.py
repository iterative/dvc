import os

import pytest

from dvc.fs.s3 import S3FileSystem
from dvc.path_info import PathInfo
from dvc.remote import get_remote
from dvc.utils.fs import walk_files

remotes = [pytest.lazy_fixture(fix) for fix in ["gs", "s3"]]

FILE_WITH_CONTENTS = {
    "data1.txt": "",
    #   "empty_dir/": "",
    "empty_file": "",
    "foo": "foo",
    "data/alice": "alice",
    "data/alpha": "alpha",
    "data/subdir-file.txt": "subdir",
    "data/subdir/1": "1",
    "data/subdir/2": "2",
    "data/subdir/3": "3",
    #   "data/subdir/empty_dir/": "",
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
        #       (True, "empty_dir"),
        (False, "foo"),
        (False, "data/alice"),
        (False, "data/al"),
        (False, "data/subdir/1"),
    ]

    for expected, path in test_cases:
        assert remote.fs.isdir(remote.fs.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_exists(remote):
    test_cases = [
        (True, "data"),
        (True, "data/"),
        (True, "data/subdir"),
        #       (True, "empty_dir"),
        (True, "empty_file"),
        (True, "foo"),
        (True, "data/alice"),
        (True, "data/subdir/1"),
        (False, "data/al"),
        #       (False, "foo/"),
        (True, "data1.txt"),
    ]

    for expected, path in test_cases:
        assert remote.fs.exists(remote.fs.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_walk_files(remote):
    files = [
        remote.fs.path_info / "data/alice",
        remote.fs.path_info / "data/alpha",
        remote.fs.path_info / "data/subdir-file.txt",
        remote.fs.path_info / "data/subdir/1",
        remote.fs.path_info / "data/subdir/2",
        remote.fs.path_info / "data/subdir/3",
        remote.fs.path_info / "data/subdir/empty_file",
    ]

    assert list(remote.fs.walk_files(remote.fs.path_info / "data")) == files


@pytest.mark.parametrize("remote", [pytest.lazy_fixture("s3")], indirect=True)
def test_copy_preserve_etag_across_buckets(remote, dvc):
    s3 = remote.fs.s3
    s3.Bucket("another").create()

    another = S3FileSystem(dvc, {"url": "s3://another", "region": "us-east-1"})

    from_info = remote.fs.path_info / "foo"
    to_info = another.path_info / "foo"

    remote.fs.copy(from_info, to_info)

    from_hash = remote.fs.info(from_info)["etag"]
    to_hash = another.info(to_info)["etag"]

    assert from_hash == to_hash


@pytest.mark.parametrize(
    "remote",
    [
        pytest.lazy_fixture("s3"),
        pytest.param(
            pytest.lazy_fixture("gs"),
            marks=pytest.mark.xfail(
                reason="https://github.com/iterative/dvc/issues/5521"
            ),
        ),
    ],
    indirect=True,
)
def test_makedirs(remote):
    fs = remote.fs
    empty_dir = remote.fs.path_info / "empty_dir" / ""
    fs.remove(empty_dir)
    assert not fs.exists(empty_dir)
    fs.makedirs(empty_dir)
    assert fs.exists(empty_dir)
    assert fs.isdir(empty_dir)


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_isfile(remote):
    test_cases = [
        #       (False, "empty_dir/"),
        (True, "empty_file"),
        (True, "foo"),
        (True, "data/alice"),
        (True, "data/alpha"),
        (True, "data/subdir/1"),
        (True, "data/subdir/2"),
        (True, "data/subdir/3"),
        #       (False, "data/subdir/empty_dir/"),
        (True, "data/subdir/empty_file"),
        (False, "something-that-does-not-exist"),
        (False, "data/subdir/empty-file/"),
        #       (False, "empty_dir"),
    ]

    for expected, path in test_cases:
        assert remote.fs.isfile(remote.fs.path_info / path) == expected


@pytest.mark.parametrize("remote", remotes, indirect=True)
def test_download_dir(remote, tmpdir):
    path = str(tmpdir / "data")
    to_info = PathInfo(path)
    remote.fs.download(remote.fs.path_info / "data", to_info)
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
