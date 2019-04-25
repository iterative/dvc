# encoding: utf-8

from __future__ import unicode_literals

from os.path import join

from dvc.scm import SCM
from dvc.scm.git import GitTree
from dvc.scm.tree import WorkingTree

from tests.basic_env import TestDir, TestGit


class TestWorkingTree(TestDir):
    def setUp(self):
        super(TestWorkingTree, self).setUp()
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


class TestGitTree(TestGit):
    def setUp(self):
        super(TestGitTree, self).setUp()
        self.scm = SCM(self._root_dir)
        self.tree = GitTree(self.git, "master")

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


class AssertWalkEqualMixin(object):
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
            [
                (
                    join(self._root_dir, "data_dir", "data_sub_dir"),
                    [],
                    ["data_sub"],
                )
            ],
        )


class TestWalkInGit(AssertWalkEqualMixin, TestGit):
    def test_nobranch(self):
        tree = WorkingTree(self._root_dir)
        self.assertWalkEqual(
            tree.walk("."),
            [
                (
                    self._root_dir,
                    ["data_dir"],
                    ["bar", "тест", "code.py", "foo"],
                ),
                (join(self._root_dir, "data_dir"), ["data_sub_dir"], ["data"]),
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
