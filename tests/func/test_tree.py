import os
from operator import itemgetter
from os.path import join

from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.repo.tree import RepoTree
from dvc.scm import SCM
from dvc.tree.git import GitTree
from dvc.tree.local import LocalTree
from dvc.utils.fs import remove
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
        with self.tree.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with self.tree.open(self.UNICODE) as fd:
            self.assertEqual(fd.read(), self.UNICODE_CONTENTS)
        with self.assertRaises(IOError):
            self.tree.open("not-existing-file")
        with self.assertRaises(IOError):
            self.tree.open(self.DATA_DIR)

    def test_exists(self):
        self.assertFalse(self.tree.exists(self.FOO))
        self.assertFalse(self.tree.exists(self.UNICODE))
        self.assertFalse(self.tree.exists(self.DATA_DIR))
        self.scm.add([self.FOO, self.UNICODE, self.DATA])
        self.scm.commit("add")
        self.assertTrue(self.tree.exists(self.FOO))
        self.assertTrue(self.tree.exists(self.UNICODE))
        self.assertTrue(self.tree.exists(self.DATA_DIR))
        self.assertFalse(self.tree.exists("non-existing-file"))

    def test_isdir(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")
        self.assertTrue(self.tree.isdir(self.DATA_DIR))
        self.assertFalse(self.tree.isdir(self.FOO))
        self.assertFalse(self.tree.isdir("non-existing-file"))

    def test_isfile(self):
        self.scm.add([self.FOO, self.DATA_DIR])
        self.scm.commit("add")
        self.assertTrue(self.tree.isfile(self.FOO))
        self.assertFalse(self.tree.isfile(self.DATA_DIR))
        self.assertFalse(self.tree.isfile("not-existing-file"))


class TestGitTree(TestGit, GitTreeTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)
        self.tree = GitTree(self.git, "master")


class TestGitSubmoduleTree(TestGitSubmodule, GitTreeTests):
    def setUp(self):
        super().setUp()
        self.scm = SCM(self._root_dir)
        self.tree = GitTree(self.git, "master")
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
        tree = GitTree(self.git, "master")
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


def test_repotree_walk_fetch(tmp_dir, dvc, scm, local_remote):
    out = tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="init")[0].outs[0]
    dvc.push()
    remove(dvc.cache.local.cache_dir)

    tree = RepoTree(dvc, fetch=True)
    with dvc.state:
        for _, _, _ in tree.walk("dir"):
            pass

    assert os.path.exists(out.cache_path)
    for entry in out.dir_cache:
        hash_ = entry[out.tree.PARAM_CHECKSUM]
        assert os.path.exists(dvc.cache.local.tree.hash_to_path_info(hash_))


def test_repotree_cache_save(tmp_dir, dvc, scm, erepo_dir, local_cloud):
    with erepo_dir.chdir():
        erepo_dir.gen({"dir": {"subdir": {"foo": "foo"}, "bar": "bar"}})
        erepo_dir.dvc_add("dir/subdir", commit="subdir")
        erepo_dir.scm_add("dir", commit="dir")
        erepo_dir.add_remote(config=local_cloud.config)
        erepo_dir.dvc.push()

    # test only cares that either fetch or stream are set so that DVC dirs are
    # walked.
    #
    # for this test, all file objects are being opened() and copied from tree
    # into dvc.cache, not fetched or streamed from a remote
    tree = RepoTree(erepo_dir.dvc, stream=True)
    expected = [
        tree.get_file_hash(PathInfo(erepo_dir / path))[1]
        for path in ("dir/bar", "dir/subdir/foo")
    ]

    with erepo_dir.dvc.state:
        cache = dvc.cache.local
        with cache.tree.state:
            path_info = PathInfo(erepo_dir / "dir")
            hash_info = cache.tree.save_info(path_info)
            cache.save(path_info, tree, hash_info)

    for hash_ in expected:
        assert os.path.exists(cache.tree.hash_to_path_info(hash_))


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

    kw = dict(ignore_subrepos=False)
    assert get_dirs(next(dvc_tree.walk(path, **kw))) == ["subdir"]
    assert get_dirs(next(scm_tree.walk(path, **kw))) == ["subdir"]
