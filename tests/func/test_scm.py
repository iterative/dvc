from __future__ import unicode_literals

import os

import pytest
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
        G.repo.index.remove([self.FOO], working_tree=True)
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


class TestIgnore(object):
    @staticmethod
    def _count_gitignore_entries(string):
        lines = get_gitignore_content()
        return len([i for i in lines if i == string])

    def test_ignore(self, git, repo_dir):
        git = Git(repo_dir._root_dir)
        foo = os.path.join(repo_dir._root_dir, repo_dir.FOO)

        target = "/" + repo_dir.FOO

        git.ignore(foo)
        assert os.path.isfile(Git.GITIGNORE)
        assert self._count_gitignore_entries(target) == 1

        git.ignore(foo)
        assert os.path.isfile(Git.GITIGNORE)
        assert self._count_gitignore_entries(target) == 1

        git.ignore_remove(foo)
        assert self._count_gitignore_entries(target) == 0

    def test_get_gitignore(self, git, repo_dir):
        data_dir = os.path.join(repo_dir._root_dir, "file1")
        entry, gitignore = Git(repo_dir._root_dir)._get_gitignore(data_dir)
        assert entry == "/file1"
        assert gitignore == os.path.join(repo_dir._root_dir, Git.GITIGNORE)

        data_dir = os.path.join(repo_dir._root_dir, "dir")
        entry, gitignore = Git(repo_dir._root_dir)._get_gitignore(data_dir)

        assert entry == "/dir"
        assert gitignore == os.path.join(repo_dir._root_dir, Git.GITIGNORE)

    def test_get_gitignore_subdir(self, git, repo_dir):
        data_dir = os.path.join(
            repo_dir._root_dir, os.path.join("dir1", "file1")
        )
        entry, gitignore = Git(repo_dir._root_dir)._get_gitignore(data_dir)
        assert entry == "/file1"
        assert gitignore == os.path.join(
            repo_dir._root_dir, "dir1", Git.GITIGNORE
        )

        data_dir = os.path.join(
            repo_dir._root_dir, os.path.join("dir1", "dir2")
        )
        entry, gitignore = Git(repo_dir._root_dir)._get_gitignore(data_dir)
        assert entry == "/dir2"
        assert gitignore == os.path.join(
            repo_dir._root_dir, "dir1", Git.GITIGNORE
        )

    def test_get_gitignore_ignorefile_dir(self, git, repo_dir):
        git = Git(repo_dir._root_dir)

        file_double_dir = os.path.join("dir1", "dir2", "file1")
        data_dir1 = os.path.join(repo_dir._root_dir, file_double_dir)
        dir1_real1 = os.path.realpath("dir1")
        entry, gitignore = git._get_gitignore(data_dir1, dir1_real1)
        assert entry == "/dir2/file1"
        gitignore1 = os.path.join(repo_dir._root_dir, "dir1", Git.GITIGNORE)
        assert gitignore == gitignore1

        triple_dir = os.path.join("dir1", "dir2", "dir3")
        data_dir2 = os.path.join(repo_dir._root_dir, triple_dir)
        dir1_real2 = os.path.realpath("dir1")
        entry, gitignore = git._get_gitignore(data_dir2, dir1_real2)
        assert entry == "/dir2/dir3"
        gitignore2 = os.path.join(repo_dir._root_dir, "dir1", Git.GITIGNORE)
        assert gitignore == gitignore2

    def test_get_gitignore_ignorefile_dir_upper_level(self, git, repo_dir):
        git = Git(repo_dir._root_dir)

        file_double_dir = os.path.join("dir1", "dir2", "file1")
        data_dir1 = os.path.join(repo_dir._root_dir, file_double_dir)
        ignore_file_dir = os.path.realpath(os.path.join("aa", "bb"))

        with pytest.raises(FileNotInTargetSubdirError):
            git._get_gitignore(data_dir1, ignore_file_dir)

    def test_gitignore_should_end_with_newline(self, git, repo_dir):
        git = Git(repo_dir._root_dir)

        foo = os.path.join(repo_dir._root_dir, repo_dir.FOO)
        bar = os.path.join(repo_dir._root_dir, repo_dir.BAR)
        gitignore = os.path.join(repo_dir._root_dir, Git.GITIGNORE)

        git.ignore(foo)

        with open(gitignore, "r") as fobj:
            last = fobj.readlines()[-1]

        assert last.endswith("\n")

        git.ignore(bar)

        with open(gitignore, "r") as fobj:
            last = fobj.readlines()[-1]

        assert last.endswith("\n")

    def test_gitignore_should_append_newline_to_gitignore(self, git, repo_dir):
        git = Git(repo_dir._root_dir)

        foo_ignore_pattern = "/foo"
        bar_ignore_pattern = "/bar"
        bar_path = os.path.join(repo_dir._root_dir, repo_dir.BAR)
        gitignore = os.path.join(repo_dir._root_dir, Git.GITIGNORE)

        with open(gitignore, "w") as fobj:
            fobj.write(foo_ignore_pattern)

        with open(gitignore, "r") as fobj:
            last = fobj.readlines()[-1]
        assert not last.endswith("\n")

        git.ignore(bar_path)

        with open(gitignore, "r") as fobj:
            lines = list(fobj.readlines())

        assert len(lines) == 2
        for l in lines:
            assert l.endswith("\n")

        assert lines[0].strip() == foo_ignore_pattern
        assert lines[1].strip() == bar_ignore_pattern
