import os

from dvc.compat import fspath

from tests.basic_env import TestDvcGit


class TestGit(TestDvcGit):
    def test_belongs_to_scm_true_on_gitignore(self):
        path = os.path.join("path", "to", ".gitignore")
        self.assertTrue(self.dvc.scm.belongs_to_scm(path))

    def test_belongs_to_scm_true_on_git_internal(self):
        path = os.path.join("path", "to", ".git", "internal", "file")
        self.assertTrue(self.dvc.scm.belongs_to_scm(path))

    def test_belongs_to_scm_false(self):
        path = os.path.join("some", "non-.git", "file")
        self.assertFalse(self.dvc.scm.belongs_to_scm(path))


def test_walk_with_submodules(tmp_dir, scm, git_dir):
    git_dir.scm_gen(
        {"foo": "foo", "bar": "bar", "dir": {"data": "data"}},
        commit="add dir and files",
    )
    scm.repo.create_submodule("submodule", "submodule", url=fspath(git_dir))
    scm.commit("added submodule")

    files = []
    dirs = []
    tree = scm.get_tree("HEAD")
    for _, dnames, fnames in tree.walk("."):
        dirs.extend(dnames)
        files.extend(fnames)

    # currently we don't walk through submodules
    assert not dirs
    assert set(files) == {".gitmodules", "submodule"}
