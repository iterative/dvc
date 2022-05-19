import os

import pytest
from funcy import first

from dvc_data.db.index import ObjectDBIndex


@pytest.fixture
def index(dvc):
    index_ = ObjectDBIndex(dvc.index_db_dir, "foo")
    yield index_


def test_init(dvc, index):
    assert str(index.index_dir) == os.path.join(dvc.tmp_dir, "index", "foo")


def test_roundtrip(dvc, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)

    new_index = ObjectDBIndex(dvc.tmp_dir, "foo")
    assert set(new_index.dir_hashes()) == expected_dir
    assert set(new_index.hashes()) == expected_dir | expected_file


def test_clear(dvc, index):
    index.update(["1234.dir"], ["5678"])
    index.clear()
    assert first(index.hashes()) is None


def test_update(dvc, index):
    expected_dir = {"1234.dir"}
    expected_file = {"5678"}
    index.update(expected_dir, expected_file)
    assert set(index.dir_hashes()) == expected_dir
    assert set(index.hashes()) == expected_dir | expected_file


def test_intersection(dvc, index):
    hashes = (str(i) for i in range(2000))
    expected = {str(i) for i in range(1000)}
    index.update([], hashes)
    assert set(index.intersection(expected)) == expected
