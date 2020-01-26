import os

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


def test_list_all_commits_detached_head(tmp_dir, scm):
    tmp_dir.scm_gen({"first": "first"}, commit="first")
    tmp_dir.scm_gen({"second": "second"}, commit="second")
    scm.branch("branch_second")
    scm.checkout("branch_third", create_new=True)
    tmp_dir.scm_gen({"third": "third"}, commit="third")
    scm.checkout("branch_second")
    assert len(scm.list_all_commits()) == 3
    # deleting the branch so that `third` commit gets lost
    scm.repo.git.branch("branch_third", D=True)
    assert len(scm.list_all_commits()) == 2
