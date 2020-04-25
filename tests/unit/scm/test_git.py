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


def test_is_tracked(tmp_dir, scm):
    tmp_dir.scm_gen(
        {
            "tracked": "tracked",
            "dir": {"data": "data", "subdir": {"subdata": "subdata"}},
        },
        commit="add dirs and files",
    )
    tmp_dir.gen({"untracked": "untracked", "dir": {"untracked": "untracked"}})

    # sanity check
    assert (tmp_dir / "untracked").exists()
    assert (tmp_dir / "tracked").exists()
    assert (tmp_dir / "dir" / "untracked").exists()
    assert (tmp_dir / "dir" / "data").exists()
    assert (tmp_dir / "dir" / "subdir" / "subdata").exists()

    assert not scm.is_tracked("untracked")
    assert not scm.is_tracked(os.path.join("dir", "untracked"))

    assert scm.is_tracked("tracked")
    assert scm.is_tracked("dir")
    assert scm.is_tracked(os.path.join("dir", "data"))
    assert scm.is_tracked(os.path.join("dir", "subdir"))
    assert scm.is_tracked(os.path.join("dir", "subdir", "subdata"))


def test_is_tracked_unicode(tmp_dir, scm):
    tmp_dir.scm_gen("ṭṝḁḉḵḗḋ", "tracked", commit="add unicode")
    tmp_dir.gen("ṳṋṭṝḁḉḵḗḋ", "untracked")
    assert scm.is_tracked("ṭṝḁḉḵḗḋ")
    assert not scm.is_tracked("ṳṋṭṝḁḉḵḗḋ")
