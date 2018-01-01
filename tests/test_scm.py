import os

from git import Repo

from dvc.scm import SCM, Base, Git

from tests.basic_env import TestDir, TestGit


class TestSCM(TestDir):
    def test_none(self):
        self.assertIsInstance(SCM(self._root_dir), Base)

    def test_git(self):
        Repo.init(os.curdir)
        self.assertIsInstance(SCM(self._root_dir), Git)


class TestSCMGit(TestGit):
    def test_is_repo(self):
        self.assertTrue(Git.is_repo(os.curdir))

    def test_ignore(self):
        Git(self._root_dir).ignore('foo')
        self.assertTrue(os.path.isfile(Git.GITIGNORE))
        self.assertTrue('foo' in open(Git.GITIGNORE, 'r').readlines())

    def test_commit(self):
        G = Git(self._root_dir)
        G.add(['foo'])
        G.commit('add')
        self.assertTrue('foo' in self.git.git.ls_files())
