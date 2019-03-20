from __future__ import unicode_literals

import os

from git import Repo

from dvc.utils.compat import str  # noqa: F401
from dvc.scm import SCM, NoSCM, Git
from dvc.scm.base import FileNotInTargetSubdirError

from tests.basic_env import TestDir, TestGit, TestGitSubmodule
from tests.utils import get_gitignore_content


class TestSCM(TestDir):
    def test_none(self):
        self.assertIsInstance(SCM(self._root_dir), NoSCM)

    def test_git(self):
        Repo.init(os.curdir)
        self.assertIsInstance(SCM(self._root_dir), Git)


class TestSCMGit(TestGit):
    def test_is_repo(self):
        self.assertTrue(Git.is_repo(os.curdir))

    def test_commit(self):
        G = Git(self._root_dir)
        G.add(["foo"])
        G.commit("add")
        self.assertIn("foo", self.git.git.ls_files())

    def test_is_tracked(self):
        foo = os.path.abspath(self.FOO)
        G = Git(self._root_dir)
        G.add([self.FOO, self.UNICODE])
        self.assertTrue(G.is_tracked(foo))
        self.assertTrue(G.is_tracked(self.FOO))
        self.assertTrue(G.is_tracked(self.UNICODE))
        G.commit("add")
        self.assertTrue(G.is_tracked(foo))
        self.assertTrue(G.is_tracked(self.FOO))
        G.git.index.remove([self.FOO], working_tree=True)
        self.assertFalse(G.is_tracked(foo))
        self.assertFalse(G.is_tracked(self.FOO))
        self.assertFalse(G.is_tracked("not-existing-file"))


class TestSCMGitSubmodule(TestGitSubmodule):
    def test_git_submodule(self):
        self.assertIsInstance(SCM(os.curdir), Git)

    def test_is_submodule(self):
        self.assertTrue(Git.is_submodule(os.curdir))

    def test_commit_in_submodule(self):
        G = Git(self._root_dir)
        G.add(["foo"])
        G.commit("add")
        self.assertTrue("foo" in self.git.git.ls_files())


class TestIgnore(TestGit):
    def _count_gitignore(self):
        lines = get_gitignore_content()
        return len([i for i in lines if i == "/" + self.FOO])

    def test_ignore(self):
        git = Git(self._root_dir)
        foo = os.path.join(self._root_dir, self.FOO)

        git.ignore(foo)
        self.assertTrue(os.path.isfile(Git.GITIGNORE))
        self.assertEqual(self._count_gitignore(), 1)

        git.ignore(foo)
        self.assertEqual(self._count_gitignore(), 1)

        git.ignore_remove(foo)
        self.assertEqual(self._count_gitignore(), 0)

    def test_get_gitignore(self):
        data_dir = os.path.join(self._root_dir, "file1")
        entry, gitignore = Git(self._root_dir)._get_gitignore(data_dir)
        self.assertEqual(entry, "/file1")
        self.assertEqual(
            gitignore, os.path.join(self._root_dir, Git.GITIGNORE)
        )

        data_dir = os.path.join(self._root_dir, "dir")
        entry, gitignore = Git(self._root_dir)._get_gitignore(data_dir)
        self.assertEqual(entry, "/dir")
        self.assertEqual(
            gitignore, os.path.join(self._root_dir, Git.GITIGNORE)
        )

    def test_get_gitignore_subdir(self):
        data_dir = os.path.join(self._root_dir, os.path.join("dir1", "file1"))
        entry, gitignore = Git(self._root_dir)._get_gitignore(data_dir)
        self.assertEqual(entry, "/file1")
        self.assertEqual(
            gitignore, os.path.join(self._root_dir, "dir1", Git.GITIGNORE)
        )

        data_dir = os.path.join(self._root_dir, os.path.join("dir1", "dir2"))
        entry, gitignore = Git(self._root_dir)._get_gitignore(data_dir)
        self.assertEqual(entry, "/dir2")
        self.assertEqual(
            gitignore, os.path.join(self._root_dir, "dir1", Git.GITIGNORE)
        )

    def test_get_gitignore_ignorefile_dir(self):
        git = Git(self._root_dir)

        file_double_dir = os.path.join("dir1", "dir2", "file1")
        data_dir1 = os.path.join(self._root_dir, file_double_dir)
        dir1_real1 = os.path.realpath("dir1")
        entry, gitignore = git._get_gitignore(data_dir1, dir1_real1)
        self.assertEqual(entry, "/dir2/file1")
        gitignore1 = os.path.join(self._root_dir, "dir1", Git.GITIGNORE)
        self.assertEqual(gitignore, gitignore1)

        triple_dir = os.path.join("dir1", "dir2", "dir3")
        data_dir2 = os.path.join(self._root_dir, triple_dir)
        dir1_real2 = os.path.realpath("dir1")
        entry, gitignore = git._get_gitignore(data_dir2, dir1_real2)
        self.assertEqual(entry, "/dir2/dir3")
        gitignore2 = os.path.join(self._root_dir, "dir1", Git.GITIGNORE)
        self.assertEqual(gitignore, gitignore2)

    def test_get_gitignore_ignorefile_dir_upper_level(self):
        git = Git(self._root_dir)

        file_double_dir = os.path.join("dir1", "dir2", "file1")
        data_dir1 = os.path.join(self._root_dir, file_double_dir)
        ignore_file_dir = os.path.realpath(os.path.join("aa", "bb"))

        with self.assertRaises(FileNotInTargetSubdirError):
            git._get_gitignore(data_dir1, ignore_file_dir)
