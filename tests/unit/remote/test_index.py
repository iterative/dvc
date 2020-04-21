import os

import pytest
from funcy import first

from dvc.remote.index import RemoteIndex


@pytest.fixture(scope="function")
def index(dvc):
    index = RemoteIndex(dvc, "foo")
    index.load()
    yield index
    index.dump()
    os.unlink(index.path)


def test_init(dvc, index):
    assert str(index.path) == os.path.join(dvc.index_dir, "foo.idx")


def test_is_dir_checksum(dvc, index):
    assert index.is_dir_checksum("foo.dir")
    assert not index.is_dir_checksum("foo")


def test_roundtrip(dvc, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)
    index.dump()
    index.load()
    assert set(index.dir_checksums()) == expected_dir
    assert set(index.checksums()) == expected_dir | expected_file


def test_clear(dvc, index):
    index.update(
        ["1234.dir"], ["5678"],
    )
    index.clear()
    assert first(index.checksums()) is None


def test_update(dvc, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)
    assert set(index.dir_checksums()) == expected_dir
    assert set(index.checksums()) == expected_dir | expected_file


def test_intersection(dvc, index):
    checksums = (str(i) for i in range(2000))
    expected = {str(i) for i in range(1000)}
    index.update([], checksums)
    assert set(index.intersection(expected)) == expected
