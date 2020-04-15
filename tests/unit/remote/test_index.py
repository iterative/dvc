import pickle
import os.path

from dvc.remote.index import RemoteIndex


def test_init(dvc):
    index = RemoteIndex(dvc, "foo")
    assert str(index.path) == os.path.join(dvc.index_dir, "foo.idx")


def test_load(dvc):
    checksums = {1, 2, 3}
    path = os.path.join(dvc.index_dir, "foo.idx")
    with open(path, "wb") as fd:
        pickle.dump(checksums, fd)
    index = RemoteIndex(dvc, "foo")
    assert index.checksums == checksums


def test_save(dvc):
    index = RemoteIndex(dvc, "foo")
    expected_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(["fedcba9876543210fedcba9876543210"])
    index.replace(expected_dir, expected_file)
    index.save()
    path = os.path.join(dvc.index_dir, "foo.idx")
    with open(path, "rb") as fd:
        checksums = pickle.load(fd)
    assert index.checksums == checksums


def test_invalidate(dvc):
    index = RemoteIndex(dvc, "foo")
    index.replace(
        ["0123456789abcdef0123456789abcdef.dir"],
        ["fedcba9876543210fedcba9876543210"],
    )
    index.save()
    index.invalidate()
    assert not index.checksums
    assert not os.path.exists(index.path)


def test_replace(dvc):
    index = RemoteIndex(dvc, "foo")
    index._dir_checksums = set()
    index._file_checksums = set()
    expected_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(["fedcba9876543210fedcba9876543210"])
    index.replace(expected_dir, expected_file)
    assert index._dir_checksums == expected_dir
    assert index._file_checksums == expected_file


def test_replace_all(dvc):
    index = RemoteIndex(dvc, "foo")
    index._dir_checksums = set()
    index._file_checksums = set()
    expected_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(["fedcba9876543210fedcba9876543210"])
    index.replace_all(expected_dir | expected_file)
    assert index._dir_checksums == expected_dir
    assert index._file_checksums == expected_file


def test_update(dvc):
    index = RemoteIndex(dvc, "foo")
    initial_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    initial_file = frozenset(["fedcba9876543210fedcba9876543210"])
    index.replace(initial_dir, initial_file)
    expected_dir = frozenset(["1123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(["fedcba9876543210fedcba9876543211"])
    index.update(expected_dir, expected_file)
    assert index._dir_checksums == initial_dir | expected_dir
    assert index._file_checksums == initial_file | expected_file


def test_update_all(dvc):
    index = RemoteIndex(dvc, "foo")
    initial_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    initial_file = frozenset(["fedcba9876543210fedcba9876543210"])
    index.replace(initial_dir, initial_file)
    expected_dir = frozenset(["1123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(["fedcba9876543210fedcba9876543211"])
    index.update_all(expected_dir | expected_file)
    assert index._dir_checksums == initial_dir | expected_dir
    assert index._file_checksums == initial_file | expected_file
