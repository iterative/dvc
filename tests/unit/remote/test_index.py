import os.path

from dvc.remote.index import dump, load, RemoteIndex


def test_protocol_v1_roundtrip(tmp_dir):
    tmpfile = os.path.join(tmp_dir, "foo.idx")

    expected_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(map(_to_checksum, range(10000)))
    with open(tmpfile, "wb") as fobj:
        dump(expected_dir, expected_file, fobj)
    with open(tmpfile, "rb") as fobj:
        dir_checksums, file_checksums = load(fobj, dir_suffix=".dir")
    assert dir_checksums == expected_dir
    assert not expected_file ^ file_checksums


def _to_checksum(n):
    return "{:032}".format(n)


def test_init(dvc):
    index = RemoteIndex(dvc, "foo")
    assert str(index.path) == os.path.join(dvc.index_dir, "foo.idx")


def test_roundtrip(dvc):
    expected_dir = frozenset(["0123456789abcdef0123456789abcdef.dir"])
    expected_file = frozenset(["fedcba9876543210fedcba9876543210"])
    index = RemoteIndex(dvc, "foo")
    index.replace(expected_dir | expected_file)
    index.save()
    index.load()
    assert index._dir_checksums == expected_dir
    assert index._file_checksums == expected_file
    assert index.checksums == expected_dir | expected_file


def test_invalidate(dvc):
    index = RemoteIndex(dvc, "foo")
    index.replace({_to_checksum(n) for n in (1, 2, 3)})
    index.save()
    index.invalidate()
    assert not index.checksums
    assert not os.path.exists(index.path)
