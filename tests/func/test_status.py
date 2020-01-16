import os
import shutil

from mock import patch

from dvc.repo import Repo
from dvc.main import main
from dvc.compat import fspath
from dvc.external_repo import clean_repos
from tests.basic_env import TestDvc


class TestStatus(TestDvc):
    def test_quiet(self):
        self.dvc.add(self.FOO)

        ret = main(["status", "--quiet"])
        self.assertEqual(ret, 0)

        os.remove(self.FOO)
        os.rename(self.BAR, self.FOO)

        ret = main(["status", "--quiet"])
        self.assertEqual(ret, 1)

    @patch("dvc.repo.status._cloud_status", return_value=True)
    def test_implied_cloud(self, mock_status):
        main(["status", "--remote", "something"])
        mock_status.assert_called()


def test_status_non_dvc_repo_import(tmp_dir, dvc, erepo_dir):
    with erepo_dir.branch("branch", new=True), erepo_dir.chdir():
        erepo_dir.scm.repo.index.remove([".dvc"], r=True)
        shutil.rmtree(".dvc")
        erepo_dir.scm_gen("file", "first version")
        erepo_dir.scm.add(["file"])
        erepo_dir.scm.commit("first version")

    dvc.imp(fspath(erepo_dir), "file", "file", rev="branch")

    status = dvc.status(["file.dvc"])

    assert status == {}

    # Caching in external repos doesn't see upstream updates within single
    # cli call, so we need to clean the caches to see the changes.
    clean_repos()

    with erepo_dir.branch("branch", new=False), erepo_dir.chdir():
        erepo_dir.scm_gen("file", "second_version", commit="update file")
        erepo_dir.scm.add(["file"])
        erepo_dir.scm.commit("first version")

    status, = dvc.status(["file.dvc"])["file.dvc"]

    assert status == {
        "changed deps": {
            "file ({})".format(fspath(erepo_dir)): "update available"
        }
    }


def test_status_before_and_after_dvc_init(tmp_dir, dvc, erepo_dir):
    with erepo_dir.chdir():
        erepo_dir.scm.repo.index.remove([".dvc"], r=True)
        shutil.rmtree(".dvc")
        erepo_dir.scm_gen("file", "first version")
        erepo_dir.scm.add(["file"])
        erepo_dir.scm.commit("first version")
        old_rev = erepo_dir.scm.get_rev()

    dvc.imp(fspath(erepo_dir), "file", "file")

    assert dvc.status(["file.dvc"]) == {}

    with erepo_dir.chdir():
        Repo.init()
        erepo_dir.scm.repo.index.remove(["file"])
        os.remove("file")
        erepo_dir.dvc_gen("file", "second version")
        erepo_dir.scm.add([".dvc", "file.dvc"])
        erepo_dir.scm.commit("version with dvc")
        new_rev = erepo_dir.scm.get_rev()

    assert old_rev != new_rev

    # Caching in external repos doesn't see upstream updates within single
    # cli call, so we need to clean the caches to see the changes.
    clean_repos()

    status, = dvc.status(["file.dvc"])["file.dvc"]

    assert status == {
        "changed deps": {
            "file ({})".format(fspath(erepo_dir)): "update available"
        }
    }
