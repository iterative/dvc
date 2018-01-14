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

    def test_commit(self):
        G = Git(self._root_dir)
        G.add(['foo'])
        G.commit('add')
        self.assertTrue('foo' in self.git.git.ls_files())


class TestIgnore(TestGit):
    def _count_gitignore(self):
        with open(Git.GITIGNORE, 'r') as fd:
            lines = fd.readlines()
            return len(list(filter(lambda x: x.strip() == 'foo', lines)))

    def test(self):
        git = Git(self._root_dir)

        git.ignore('foo')
        self.assertTrue(os.path.isfile(Git.GITIGNORE))
        self.assertEqual(self._count_gitignore(), 1)

        git.ignore('foo')
        self.assertEqual(self._count_gitignore(), 1)

        git.ignore_remove('foo')
        self.assertEqual(self._count_gitignore(), 0)
