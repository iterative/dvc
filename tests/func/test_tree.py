import os
from os.path import join

from dvc.ignore import CleanTree
from dvc.path_info import PathInfo
from dvc.repo.tree import RepoTree
from dvc.scm import SCM
from dvc.scm.git import GitTree
from dvc.scm.tree import WorkingTree
from dvc.utils.fs import remove
from tests.basic_env import TestDir, TestGit, TestGitSubmodule


class TestWorkingTree(TestDir):
    def setUp(self):
        super().setUp()
        self.tree = WorkingTree()

    def test_open(self):
        with self.tree.open(self.FOO) as fd:
            self.assertEqual(fd.read(), self.FOO_CONTENTS)
        with self.tree.open(self.UNICODE) as fd:
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
        tree = WorkingTree(self._root_dir)
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
        tree = WorkingTree(self._root_dir)
        self.assertWalkEqual(
            tree.walk(join("data_dir", "data_sub_dir")),
            [(join("data_dir", "data_sub_dir"), [], ["data_sub"])],
        )


class TestWalkInGit(AssertWalkEqualMixin, TestGit):
    def test_nobranch(self):
        tree = CleanTree(WorkingTree(self._root_dir))
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


def test_repotree_walk_fetch(tmp_dir, dvc, scm, setup_remote):
    setup_remote(dvc)
    out = tmp_dir.dvc_gen({"dir": {"foo": "foo"}}, commit="init")[0].outs[0]
    dvc.push()
    remove(dvc.cache.local.cache_dir)

    tree = RepoTree(dvc, fetch=True)
    with dvc.state:
        for _, _, _ in tree.walk("dir"):
            pass

    assert os.path.exists(out.cache_path)
    for entry in out.dir_cache:
        checksum = entry[out.remote.PARAM_CHECKSUM]
        assert os.path.exists(dvc.cache.local.checksum_to_path_info(checksum))


def test_repotree_cache_save(tmp_dir, dvc, scm, erepo_dir, setup_remote):
    with erepo_dir.chdir():
        erepo_dir.gen({"dir": {"subdir": {"foo": "foo"}, "bar": "bar"}})
        erepo_dir.dvc_add("dir/subdir", commit="subdir")
        erepo_dir.scm_add("dir", commit="dir")
        setup_remote(erepo_dir.dvc)
        erepo_dir.dvc.push()

    # test only cares that either fetch or stream are set so that DVC dirs are
    # walked.
    #
    # for this test, all file objects are being opened() and copied from tree
    # into dvc.cache, not fetched or streamed from a remote
    tree = RepoTree(erepo_dir.dvc, stream=True)
    expected = [
        tree.get_file_checksum(erepo_dir / path)
        for path in ("dir/bar", "dir/subdir/foo")
    ]

    with erepo_dir.dvc.state:
        cache = dvc.cache.local
        with cache.state:
            cache.save(PathInfo(erepo_dir / "dir"), None, tree=tree)
    for checksum in expected:
        assert os.path.exists(cache.checksum_to_path_info(checksum))
