import os
import shutil

from dvc.repo.tree import DvcTree
from dvc.compat import fspath_py35


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    tree = DvcTree(dvc)
    assert tree.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    tree = DvcTree(dvc)
    with tree.open("foo", "r") as fobj:
        assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    tree = DvcTree(dvc)
    assert not tree.isdir("datadir")
    assert not tree.isfile("datadir")
    assert not tree.isdir("datafile")
    assert not tree.isfile("datafile")

    dvc.add(["datadir", "datafile"])
    shutil.rmtree(fspath_py35(tmp_dir / "datadir"))
    (tmp_dir / "datafile").unlink()

    assert tree.isdir("datadir")
    assert not tree.isfile("datadir")
    assert not tree.isdir("datafile")
    assert tree.isfile("datafile")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    tree = DvcTree(dvc)
    assert tree.isdir("dir")
    assert not tree.isfile("dir")


def test_walk(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
                "foo": "foo",
                "bar": "bar",
            }
        }
    )

    dvc.add("dir", recursive=True)
    tree = DvcTree(dvc)

    expected = [
        str(tmp_dir / "dir" / "subdir1"),
        str(tmp_dir / "dir" / "subdir2"),
        str(tmp_dir / "dir" / "subdir1" / "foo1"),
        str(tmp_dir / "dir" / "subdir1" / "bar1"),
        str(tmp_dir / "dir" / "subdir2" / "foo2"),
        str(tmp_dir / "dir" / "foo"),
        str(tmp_dir / "dir" / "bar"),
    ]

    actual = []
    for root, dirs, files in tree.walk("dir"):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    dvc.add("foo")
    tree = DvcTree(dvc)
    assert tree.isdvc("foo")
    assert not tree.isdvc("bar")
