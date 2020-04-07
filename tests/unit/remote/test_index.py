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
    index.replace({4, 5, 6})
    index.save()
    path = os.path.join(dvc.index_dir, "foo.idx")
    with open(path, "rb") as fd:
        checksums = pickle.load(fd)
    assert index.checksums == checksums


def test_invalidate(dvc):
    index = RemoteIndex(dvc, "foo")
    index.replace({1, 2, 3})
    index.save()
    index.invalidate()
    assert not index.checksums
    assert not index.path.exists()
