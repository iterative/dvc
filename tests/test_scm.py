import os

from git import Repo
import hglib

from dvc.scm import SCM, Base, Git, Mercurial

from tests.basic_env import (TestDir, TestGit, TestGitSubmodule,
                             TestMercurial)


class TestSCM(TestDir):
    def test_none(self):
        self.assertIsInstance(SCM(self._root_dir), Base)

    def test_git(self):
        Repo.init(os.curdir)
        self.assertIsInstance(SCM(self._root_dir), Git)
    
    def test_hg(self):
        hglib.init(os.curdir)
        self.assertIsInstance(SCM(self._root_dir), Mercurial)


class TestSCMGit(TestGit):
    def test_is_repo(self):
        self.assertTrue(Git.is_repo(os.curdir))

    def test_commit(self):
        G = Git(self._root_dir)
        G.add(['foo'])
        G.commit('add')
        self.assertTrue('foo' in self.git.git.ls_files())


class TestSCMGitSubmodule(TestGitSubmodule):
    def test_git_submodule(self):
        self.assertIsInstance(SCM(os.curdir), Git)

    def test_is_submodule(self):
        self.assertTrue(Git.is_submodule(os.curdir))

    def test_commit_in_submodule(self):
        G = Git(self._root_dir)
        G.add(['foo'])
        G.commit('add')
        self.assertTrue('foo' in self.git.git.ls_files())

class TestSCMMercurial(TestMercurial):
    def test_is_repo(self):
        self.assertTrue(Mercurial.is_repo(os.curdir))

    def test_commit(self):
        hg = Mercurial(self._root_dir)
        hg.add(['foo'])
        hg.commit('add')
        self.assertTrue('foo' in [fname for (_, _, _, _, fname) in self.hg.manifest()])


class TestIgnore(TestGit):
    def _count_gitignore(self):
        with open(Git.GITIGNORE, 'r') as fd:
            lines = fd.readlines()
            return len(list(filter(lambda x: x.strip() == 'foo', lines)))

    def test(self):
        git = Git(self._root_dir)
        foo = os.path.join(self._root_dir, self.FOO)

        git.ignore(foo)
        self.assertTrue(os.path.isfile(Git.GITIGNORE))
        self.assertEqual(self._count_gitignore(), 1)

        git.ignore(foo)
        self.assertEqual(self._count_gitignore(), 1)

        git.ignore_remove(foo)
        self.assertEqual(self._count_gitignore(), 0)
