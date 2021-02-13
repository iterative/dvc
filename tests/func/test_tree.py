import os
from operator import itemgetter
from os.path import join

import pytest

from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.scm import SCM
from dvc.tree import get_cloud_tree
from dvc.tree.local import LocalTree
from tests.basic_env import TestDir, TestGit, TestGitSubmodule


class TestLocalTree(TestDir):
    def setUp(self):
        super().setUp()
        self.tree = LocalTree(None, {})

    def test_open(self):
        with self.tree.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with self.tree.open(self.UNICODE, encoding="utf-8") as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)

    def test_exists(self):
        self.assertTrue(self.tree.exists(self.FOO))
        self.assertTrue(self.tree.exists(self.UNICODE))
        self.assertFalse(self.tree.exists("not-existing-file"))

    def test_isdir(self):
        self.assertTrue(self.tree.isdir(self.DATA_DIR))
        self.assertFalse(self.tree.isdir(self.FOO))
        self.assertFalse(self.tree.isdir("not-existing-file"))

    def test_isfile(self):
        self.assertTrue(self.tree.isfile(self.FOO))
        self.assertFalse(self.tree.isfile(self.DATA_DIR))
        self.assertFalse(self.tree.isfile("not-existing-file"))


class GitTreeTests:
    # pylint: disable=no-member
    def test_open(self):
        self.scm.add([self.FOO, self.UNICODE, self.DATA_DIR])
        self.scm.commit("add")

        tree = self.scm.get_tree("master")
        with tree.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with tree.open(self.UNICODE) as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)
        with self.assertRaises(IOError):
            tree.open("not-existing-file")
        with self.assertRaises(IOError):
            tree.open(self.DATA_DIR)

    def test_exists(self):
        tree = self.scm.get_tree("master")
        self.assertFalse(tree.exists(self.FOO))
        self.assertFalse(tree.exists(self.UNICODE))
        self.assertFalse(tree.exists(self.DATA_DIR))
        self.scm.add([self.FOO, self.UNICODE, self.DATA])
        self.scm.commit("add")

        tree = self.scm.get_tree("master")
        self.assertTrue(tree.exists(self.FOO))
        self.assertTrue(tree.exists(self.UNICODE))
        self.assertTrue(tree.exists(self.DATA_DIR))
        self.assertFalse(tree.exists("non-existing-file"))

    def test_isdir(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")

        tree = self.scm.get_tree("master")
        self.assertTrue(tree.isdir(self.DATA_DIR))
        self.assertFalse(tree.isdir(self.FOO))
        self.assertFalse(tree.isdir("non-existing-file"))

    def test_isfile(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")

        tree = self.scm.get_tree("master")
        self.assertTrue(tree.isfile(self.FOO))
        self.assertFalse(tree.isfile(self.DATA_DIR))
        self.assertFalse(tree.isfile("not-existing-file"))


class TestGitTree(TestGit, GitTreeTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)


class TestGitSubmoduleTree(TestGitSubmodule, GitTreeTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)
        self._pushd(self._root_dir)


class AssertWalkEqualMixin:
    def assertWalkEqual(self, actual, expected, msg=None):
        def convert_to_sets(walk_results):
            return [
                (root, set(dirs), set(nondirs))
                for root, dirs, nondirs in walk_results
            ]

        self.assertEqual(
            convert_to_sets(actual), convert_to_sets(expected), msg=msg
        )


class TestWalkInNoSCM(AssertWalkEqualMixin, TestDir):
    def test(self):
        tree = LocalTree(None, {"url": self._root_dir})
        self.assertWalkEqual(
            tree.walk(self._root_dir),
            [
                (
                    self._root_dir,
                    ["data_dir"],
                    ["code.py", "bar", "тест", "foo"],
                ),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], ["data"]),
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                ),
            ],
        )

    def test_subdir(self):
        tree = LocalTree(None, {"url": self._root_dir})
        self.assertWalkEqual(
            tree.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )


class TestWalkInGit(AssertWalkEqualMixin, TestGit):
    def test_nobranch(self):
        tree = LocalTree(None, {"url": self._root_dir}, use_dvcignore=True)
        self.assertWalkEqual(
            tree.walk("."),
            [
                (".", ["data_dir"], ["bar", "тест", "code.py", "foo"]),
                (join("data_dir"), ["data_sub_dir"], ["data"]),
                (join("data_dir", "data_sub_dir"), [], ["data_sub"]),
            ],
        )
        self.assertWalkEqual(
            tree.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )

    def test_branch(self):
        scm = SCM(self._root_dir)
        scm.add([self.DATA_SUB_DIR])
        scm.commit("add data_dir/data_sub_dir/data_sub")
        tree = scm.get_tree("master")
        self.assertWalkEqual(
            tree.walk("."),
            [
                (self._root_dir, ["data_dir"], ["code.py"]),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], []),
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                ),
            ],
        )
        self.assertWalkEqual(
            tree.walk(join("data_dir", "data_sub_dir")),
            [
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                )
            ],
        )


def test_cleantree_subrepo(tmp_dir, dvc, scm, monkeypatch):
    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        subrepo = Repo.init(subdir=True)
        subrepo_dir.gen({"foo": "foo", "dir": {"bar": "bar"}})

    path = PathInfo(subrepo_dir)

    assert dvc.tree.use_dvcignore
    assert not dvc.tree.exists(path / "foo")
    assert not dvc.tree.isfile(path / "foo")
    assert not dvc.tree.exists(path / "dir")
    assert not dvc.tree.isdir(path / "dir")

    assert subrepo.tree.use_dvcignore
    assert subrepo.tree.exists(path / "foo")
    assert subrepo.tree.isfile(path / "foo")
    assert subrepo.tree.exists(path / "dir")
    assert subrepo.tree.isdir(path / "dir")


def test_walk_dont_ignore_subrepos(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="add foo")
    subrepo_dir = tmp_dir / "subdir"
    subrepo_dir.mkdir()
    with subrepo_dir.chdir():
        Repo.init(subdir=True)
    scm.add(["subdir"])
    scm.commit("Add subrepo")

    dvc_tree = dvc.tree
    dvc_tree._reset()
    scm_tree = scm.get_tree("HEAD", use_dvcignore=True)
    path = os.fspath(tmp_dir)
    get_dirs = itemgetter(1)

    assert get_dirs(next(dvc_tree.walk(path))) == []
    assert get_dirs(next(scm_tree.walk(path))) == []

    kw = {"ignore_subrepos": False}
    assert get_dirs(next(dvc_tree.walk(path, **kw))) == ["subdir"]
    assert get_dirs(next(scm_tree.walk(path, **kw))) == ["subdir"]


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("http"),
    ],
)
def test_tree_getsize(dvc, cloud):
    cloud.gen({"data": {"foo": "foo"}, "baz": "baz baz"})
    tree = get_cloud_tree(dvc, **cloud.config)
    path_info = tree.path_info

    assert tree.getsize(path_info / "baz") == 7
    assert tree.getsize(path_info / "data" / "foo") == 3


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("azure"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("gdrive"),
        pytest.lazy_fixture("hdfs"),
        pytest.lazy_fixture("http"),
        pytest.lazy_fixture("local_cloud"),
        pytest.lazy_fixture("oss"),
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("ssh"),
        pytest.lazy_fixture("webhdfs"),
    ],
)
def test_tree_upload_fobj(dvc, tmp_dir, cloud):
    tmp_dir.gen("foo", "foo")
    tree = get_cloud_tree(dvc, **cloud.config)

    from_info = tmp_dir / "foo"
    to_info = tree.path_info / "foo"

    with open(from_info, "rb") as stream:
        tree.upload_fobj(stream, to_info)

    assert tree.exists(to_info)
    with tree.open(to_info, "rb") as stream:
        assert stream.read() == b"foo"


@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("gdrive")])
def test_tree_ls(dvc, cloud):
    cloud.gen(
        {
            "directory": {
                "foo": "foo",
                "bar": "bar",
                "baz": {"quux": "quux", "egg": {"foo": "foo"}},
                "empty": {},
            }
        }
    )
    tree = get_cloud_tree(dvc, **cloud.config)
    path_info = cloud / "directory"

    assert {os.path.basename(file_key) for file_key in tree.ls(path_info)} == {
        "foo",
        "bar",
        "baz",
        "empty",
    }
    assert set(tree.ls(path_info / "empty")) == set()
    assert {
        (detail["type"], os.path.basename(detail["name"]))
        for detail in tree.ls(path_info / "baz", detail=True)
    } == {("file", "quux"), ("directory", "egg")}


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("azure"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("webdav"),
        pytest.lazy_fixture("gdrive"),
    ],
)
def test_tree_ls_recursive(dvc, cloud):
    cloud.gen({"data": {"foo": "foo", "bar": {"baz": "baz"}, "quux": "quux"}})
    tree = get_cloud_tree(dvc, **cloud.config)
    path_info = tree.path_info

    assert {
        os.path.basename(file_key)
        for file_key in tree.ls(path_info / "data", recursive=True)
    } == {"foo", "baz", "quux"}


@pytest.mark.parametrize(
    "cloud",
    [
        pytest.lazy_fixture("s3"),
        pytest.lazy_fixture("azure"),
        pytest.lazy_fixture("gs"),
        pytest.lazy_fixture("webdav"),
    ],
)
def test_tree_ls_with_etag(dvc, cloud):
    cloud.gen({"data": {"foo": "foo", "bar": {"baz": "baz"}, "quux": "quux"}})
    tree = get_cloud_tree(dvc, **cloud.config)
    path_info = tree.path_info

    for details in tree.ls(path_info / "data", recursive=True, detail=True):
        assert (
            tree.info(path_info.replace(path=details["name"]))["etag"]
            == details["etag"]
        )
