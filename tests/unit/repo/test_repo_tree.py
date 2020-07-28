import os
import shutil
from unittest import mock

import pytest

from dvc.path_info import PathInfo
from dvc.repo.tree import RepoTree
from tests.func.test_get import make_subrepo


def test_exists(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    tree = RepoTree(dvc, dvc.tree)
    assert tree.exists("foo")


def test_open(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo")
    dvc.add("foo")
    (tmp_dir / "foo").unlink()

    tree = RepoTree(dvc, dvc.tree)
    with dvc.state:
        with tree.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_open_dirty_hash(tmp_dir, dvc):
    tmp_dir.dvc_gen("file", "file")
    (tmp_dir / "file").write_text("something")

    tree = RepoTree(dvc, dvc.tree)
    with tree.open("file", "r") as fobj:
        assert fobj.read() == "something"


def test_open_dirty_no_hash(tmp_dir, dvc):
    tmp_dir.gen("file", "file")
    (tmp_dir / "file.dvc").write_text("outs:\n- path: file\n")

    tree = RepoTree(dvc, dvc.tree)
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

        tree = RepoTree(dvc, dvc.tree)
        with tree.open("foo", "r") as fobj:
            assert fobj.read() == "foo"


def test_isdir_isfile(tmp_dir, dvc):
    tmp_dir.gen({"datafile": "data", "datadir": {"foo": "foo", "bar": "bar"}})

    tree = RepoTree(dvc, dvc.tree)
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

    tree = RepoTree(dvc, dvc.tree)
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
    tree = RepoTree(dvc, dvc.tree)

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
    tree = RepoTree(dvc, dvc.tree)

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
    tree = RepoTree(dvc, dvc.tree)
    assert tree.isdvc("foo")
    assert not tree.isdvc("bar")
    assert tree.isdvc("dir")
    assert not tree.isdvc("dir/baz")
    assert tree.isdvc("dir/baz", recursive=True, strict=False)


def test_in_subtree(tmp_dir, scm, dvc):
    subrepo1 = tmp_dir / "dir" / "repo"
    subrepo2 = tmp_dir / "dir" / "repo2"

    for repo in [subrepo1, subrepo2]:
        make_subrepo(repo, scm)

    (tmp_dir / "dir" / "repotxt").write_text("file to confuse RepoTree")
    subrepo1.dvc_gen({"foo": "foo"}, commit="FOO")
    subrepo2.dvc_gen({"bar": "bar"}, commit="BAR")

    # dvc.tree ignores subrepos by default,
    # but we just want to test `in_subtree()`, which is purely lexical
    tree = RepoTree(dvc, scm.get_tree("HEAD"))

    assert tree.in_subtree(str(tmp_dir / "dir")).repo == dvc
    assert tree.in_subtree(str(tmp_dir / "dir" / "re")).repo == dvc
    assert tree.in_subtree(str(tmp_dir / "dir" / "repo")).repo == subrepo1.dvc
    assert tree.in_subtree(str(tmp_dir / "dir" / "repotxt")).repo == dvc
    assert tree.in_subtree(str(tmp_dir / "dir" / "repo2")).repo == subrepo2.dvc

    for repo in [tmp_dir, subrepo1, subrepo2]:
        for path in ["", "foo", "something-that-does-not-exist"]:
            p = os.path.join(repo, path)
            subtree = tree.in_subtree(p)
            assert subtree, f"subtree not found for path '{p}'"
            assert (
                subtree.repo == repo.dvc
            ), f"repo did not match for path '{p}'"


def test_subrepos(tmp_dir, scm, dvc):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse RepoTree"}},
        commit="dir/repo.txt",
    )

    subrepo1 = tmp_dir / "dir" / "repo"
    subrepo2 = tmp_dir / "dir" / "repo2"

    for repo in [subrepo1, subrepo2]:
        make_subrepo(repo, scm)

    subrepo1.dvc_gen({"foo": "foo", "dir1": {"bar": "bar"}}, commit="FOO")
    subrepo2.dvc_gen(
        {"lorem": "lorem", "dir2": {"ipsum": "ipsum"}}, commit="BAR"
    )

    # using tree that does not have dvcignore
    tree = RepoTree(dvc, scm.get_tree("HEAD"), fetch=True)

    def assert_tree_belongs_to_repo(ret_val):
        method = tree._find_subtree

        def f(*args, **kwargs):
            r = method(*args, **kwargs)
            assert r and r.repo == ret_val
            return r

        return f

    with mock.patch.object(
        tree,
        "_find_subtree",
        side_effect=assert_tree_belongs_to_repo(subrepo1.dvc),
    ):
        assert tree.exists(subrepo1 / "foo") is True
        assert tree.exists(subrepo1 / "bar") is False

        assert tree.isfile(subrepo1 / "foo") is True
        assert tree.isfile(subrepo1 / "foo") is True
        assert tree.isfile(subrepo1 / "dir1" / "bar") is True
        assert tree.isfile(subrepo1 / "dir1") is False

        assert tree.isdir(subrepo1 / "dir1") is True
        assert tree.isdir(subrepo1 / "dir1" / "bar") is False
        assert tree.isdvc(subrepo1 / "foo") is True

    with mock.patch.object(
        tree,
        "_find_subtree",
        side_effect=assert_tree_belongs_to_repo(subrepo2.dvc),
    ):
        assert tree.exists(subrepo2 / "lorem") is True
        assert tree.exists(subrepo2 / "ipsum") is False

        assert tree.isfile(subrepo2 / "lorem") is True
        assert tree.isfile(subrepo2 / "lorem") is True
        assert tree.isfile(subrepo2 / "dir2" / "ipsum") is True
        assert tree.isfile(subrepo2 / "dir2") is False

        assert tree.isdir(subrepo2 / "dir2") is True
        assert tree.isdir(subrepo2 / "dir2" / "ipsum") is False
        assert tree.isdvc(subrepo2 / "lorem") is True


@pytest.mark.parametrize(
    "dvcfiles,extra_expected",
    [
        (False, []),
        (
            True,
            [
                PathInfo("dir") / "repo" / "foo.dvc",
                PathInfo("dir") / "repo" / "dir1.dvc",
                PathInfo("dir") / "repo2" / "lorem.dvc",
                PathInfo("dir") / "repo2" / "dir2.dvc",
            ],
        ),
    ],
)
def test_subrepo_walk(tmp_dir, scm, dvc, dvcfiles, extra_expected):
    tmp_dir.scm_gen(
        {"dir": {"repo.txt": "file to confuse RepoTree"}},
        commit="dir/repo.txt",
    )

    subrepo1 = tmp_dir / "dir" / "repo"
    subrepo2 = tmp_dir / "dir" / "repo2"

    subdirs = [subrepo1, subrepo2]
    for dir_ in subdirs:
        make_subrepo(dir_, scm)

    subrepo1.dvc_gen({"foo": "foo", "dir1": {"bar": "bar"}}, commit="FOO")
    subrepo2.dvc_gen(
        {"lorem": "lorem", "dir2": {"ipsum": "ipsum"}}, commit="BAR"
    )

    # using tree that does not have dvcignore
    tree = RepoTree(
        dvc,
        scm.get_tree("HEAD", use_dvcignore=True, ignore_subrepo=False),
        fetch=True,
    )
    expected = [
        PathInfo("dir") / "repo",
        PathInfo("dir") / "repo.txt",
        PathInfo("dir") / "repo2",
        PathInfo("dir") / "repo" / ".gitignore",
        PathInfo("dir") / "repo" / "foo",
        PathInfo("dir") / "repo" / "dir1",
        PathInfo("dir") / "repo" / "dir1" / "bar",
        PathInfo("dir") / "repo2" / ".gitignore",
        PathInfo("dir") / "repo2" / "lorem",
        PathInfo("dir") / "repo2" / "dir2",
        PathInfo("dir") / "repo2" / "dir2" / "ipsum",
    ]

    actual = []
    for root, dirs, files in tree.walk("dir", dvcfiles=dvcfiles):
        for entry in dirs + files:
            actual.append(os.path.join(root, entry))

    # need to change to abspath as scm.get_tree() makes `root` return abspath
    expected = [str(tmp_dir / path) for path in expected + extra_expected]
    assert set(actual) == set(expected)
    assert len(actual) == len(expected)
