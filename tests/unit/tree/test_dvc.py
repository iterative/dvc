import os
import shutil

import pytest

from dvc.hash_info import HashInfo
from dvc.path_info import PathInfo
from dvc.tree.dvc import DvcTree


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


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    tree = DvcTree(dvc)
    with tree.open("file", "r") as fobj:
        # NOTE: Unlike RepoTree, DvcTree should not
        # be affected by a dirty workspace.
        assert fobj.read() == "file"


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    tree = DvcTree(dvc)
    # NOTE: Unlike RepoTree, DvcTree should not
    # be affected by a dirty workspace.
    with pytest.raises(FileNotFoundError):
        with tree.open("file", "r"):
            pass


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


def test_get_hash_file(tmp_dir, dvc):
    tmp_dir.dvc_gen({"foo": "foo"})
    tree = DvcTree(dvc)
    assert tree.get_hash(PathInfo(tmp_dir) / "foo") == HashInfo(
        "md5", "acbd18db4cc2f85cedef654fccc4a4d8",
    )


def test_get_hash_dir(tmp_dir, dvc, mocker):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    tree = DvcTree(dvc)
    get_file_hash_spy = mocker.spy(tree, "get_file_hash")
    assert tree.get_hash(PathInfo(tmp_dir) / "dir") == HashInfo(
        "md5", "8761c4e9acad696bee718615e23e22db.dir",
    )
    assert not get_file_hash_spy.called


def test_get_hash_granular(tmp_dir, dvc):
    tmp_dir.dvc_gen(
        {"dir": {"foo": "foo", "bar": "bar", "subdir": {"data": "data"}}}
    )
    tree = DvcTree(dvc, fetch=True)
    subdir = PathInfo(tmp_dir) / "dir" / "subdir"
    assert tree.get_hash(subdir) == HashInfo(
        "md5", "af314506f1622d107e0ed3f14ec1a3b5.dir",
    )
    assert tree.get_hash(subdir / "data") == HashInfo(
        "md5", "8d777f385d3dfec8815d20f7496026dc",
    )


def test_get_hash_dirty_file(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    tree = DvcTree(dvc)
    actual = tree.get_hash(PathInfo(tmp_dir) / "file")
    expected = HashInfo("md5", "8c7dd922ad47494fc02c388e12c00eac")
    assert actual == expected


def test_get_hash_dirty_dir(tmp_dir, dvc):
    tmp_dir.dvc_gen({"dir": {"foo": "foo", "bar": "bar"}})
    (tmp_dir / "dir" / "baz").write_text("baz")

    tree = DvcTree(dvc)
    actual = tree.get_hash(PathInfo(tmp_dir) / "dir")
    expected = HashInfo("md5", "5ea40360f5b4ec688df672a4db9c17d1.dir")

    assert actual == expected
