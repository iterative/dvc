import os
import shutil

import pytest

from dvc.path_info import PathInfo
from dvc.repo.tree import DvcTree


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
    with dvc.state:
        with tree.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    tree = DvcTree(dvc)
    with tree.open("file", "r") as fobj:
        assert fobj.read() == "something"


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    tree = DvcTree(dvc)
    with tree.open("file", "r") as fobj:
        assert fobj.read() == "file"


def test_open_in_history(tmp_dir, scm, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    dvc.scm.add(["foo.dvc", ".gitignore"])
    dvc.scm.commit("foo")

    tmp_dir.gen("foo", "foofoo")
    dvc.add("foo")
    dvc.scm.add(["foo.dvc", ".gitignore"])
    dvc.scm.commit("foofoo")

    for rev in dvc.brancher(revs=["HEAD~1"]):
        if rev == "workspace":
            continue

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
    shutil.rmtree(tmp_dir / "datadir")
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


@pytest.mark.parametrize(
    "fetch,expected",
    [
        (False, []),
        (
            True,
            [
                PathInfo("dir") / "subdir1",
                PathInfo("dir") / "subdir2",
                PathInfo("dir") / "subdir1" / "foo1",
                PathInfo("dir") / "subdir1" / "bar1",
                PathInfo("dir") / "subdir2" / "foo2",
                PathInfo("dir") / "foo",
                PathInfo("dir") / "bar",
            ],
        ),
    ],
)
def test_walk_dir(tmp_dir, dvc, fetch, expected):
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

    dvc.add("dir")
    tree = DvcTree(dvc, fetch=fetch)

    expected = [str(tmp_dir / path) for path in expected]

    with dvc.state:
        actual = []
        for root, dirs, files in tree.walk("dir"):
            for entry in dirs + files:
                actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_onerror(tmp_dir, dvc):
    def onerror(exc):
        raise exc

    tmp_dir.dvc_gen("foo", "foo")
    tree = DvcTree(dvc)

    # path does not exist
    for _ in tree.walk("dir"):
        pass
    with pytest.raises(OSError):
        for _ in tree.walk("dir", onerror=onerror):
            pass

    # path is not a directory
    for _ in tree.walk("foo"):
        pass
    with pytest.raises(OSError):
        for _ in tree.walk("foo", onerror=onerror):
            pass


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    dvc.add("foo")
    tree = DvcTree(dvc)
    assert tree.isdvc("foo")
    assert not tree.isdvc("bar")
