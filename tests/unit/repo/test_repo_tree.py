import os
import shutil

import pytest

from dvc.path_info import PathInfo
from dvc.repo.tree import RepoTree


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    tree = RepoTree(dvc)
    assert tree.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    tree = RepoTree(dvc)
    with dvc.state:
        with tree.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    tree = RepoTree(dvc)
    with tree.open("file", "r") as fobj:
        assert fobj.read() == "something"


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    tree = RepoTree(dvc)
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

        tree = RepoTree(dvc)
        with tree.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    tree = RepoTree(dvc)
    assert tree.isdir("datadir")
    assert not tree.isfile("datadir")
    assert not tree.isdvc("datadir")
    assert not tree.isdir("datafile")
    assert tree.isfile("datafile")
    assert not tree.isdvc("datafile")

    dvc.add(["datadir", "datafile"])
    shutil.rmtree(tmp_dir / "datadir")
    (tmp_dir / "datafile").unlink()

    assert tree.isdir("datadir")
    assert not tree.isfile("datadir")
    assert tree.isdvc("datadir")
    assert not tree.isdir("datafile")
    assert tree.isfile("datafile")
    assert tree.isdvc("datafile")


def test_isdir_mixed(tmp_dir, dvc):
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})

    dvc.add(str(tmp_dir / "dir" / "foo"))

    tree = RepoTree(dvc)
    assert tree.isdir("dir")
    assert not tree.isfile("dir")


@pytest.mark.parametrize(
    "dvcfiles,extra_expected",
    [
        (False, []),
        (
            True,
            [
                PathInfo("dir") / "subdir1" / "foo1.dvc",
                PathInfo("dir") / "subdir1" / "bar1.dvc",
                PathInfo("dir") / "subdir2" / "foo2.dvc",
            ],
        ),
    ],
)
def test_walk(tmp_dir, dvc, dvcfiles, extra_expected):
    tmp_dir.gen(
        {
            "dir": {
                "subdir1": {"foo1": "foo1", "bar1": "bar1"},
                "subdir2": {"foo2": "foo2"},
            }
        }
    )
    dvc.add(str(tmp_dir / "dir"), recursive=True)
    tmp_dir.gen({"dir": {"foo": "foo", "bar": "bar"}})
    tree = RepoTree(dvc)

    expected = [
        PathInfo("dir") / "subdir1",
        PathInfo("dir") / "subdir2",
        PathInfo("dir") / "subdir1" / "foo1",
        PathInfo("dir") / "subdir1" / "bar1",
        PathInfo("dir") / "subdir2" / "foo2",
        PathInfo("dir") / "foo",
        PathInfo("dir") / "bar",
    ]

    actual = []
    for root, dirs, files in tree.walk("dir", dvcfiles=dvcfiles):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    expected = [str(path) for path in expected + extra_expected]
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)


def test_walk_onerror(tmp_dir, dvc):
    def onerror(exc):
        raise exc

    tmp_dir.dvc_gen("foo", "foo")
    tree = RepoTree(dvc)

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
    tmp_dir.gen({"foo": "foo", "bar": "bar", "dir": {"baz": "baz"}})
    dvc.add("foo")
    dvc.add("dir")
    tree = RepoTree(dvc)
    assert tree.isdvc("foo")
    assert not tree.isdvc("bar")
    assert tree.isdvc("dir")
    assert not tree.isdvc("dir/baz")
    assert tree.isdvc("dir/baz", recursive=True, strict=False)
