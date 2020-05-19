import os
import shutil

import pytest

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
    with tree.open("foo", "r") as fobj:
        assert fobj.read() == "foo"


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
        if rev == "working tree":
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
                "dir/subdir1/foo1.dvc",
                "dir/subdir1/bar1.dvc",
                "dir/subdir2/foo2.dvc",
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
        "dir/subdir1",
        "dir/subdir2",
        "dir/subdir1/foo1",
        "dir/subdir1/bar1",
        "dir/subdir2/foo2",
        "dir/foo",
        "dir/bar",
    ]

    actual = []
    for root, dirs, files in tree.walk("dir", dvcfiles=dvcfiles):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    assert set(actual) == set(expected + extra_expected)
    assert len(actual) == len(expected + extra_expected)


def test_isdvc(tmp_dir, dvc):
    tmp_dir.gen({"foo": "foo", "bar": "bar"})
    dvc.add("foo")
    tree = RepoTree(dvc)
    assert tree.isdvc("foo")
    assert not tree.isdvc("bar")
