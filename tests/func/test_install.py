import os
import sys

import pytest
from dvc.utils import file_md5

from dvc.main import main
from dvc.stage import Stage


@pytest.mark.skipif(
    sys.platform == "win32", reason="Git hooks aren't " "supported on Windows"
)
class TestInstall(object):
    def _hook(self, name):
        return os.path.join(".git", "hooks", name)

    @pytest.fixture(autouse=True)
    def setUp(self, dvc):
        ret = main(["install"])
        assert ret == 0

    def test_should_not_install_twice(self, dvc):
        ret = main(["install"])
        assert ret != 0

    def test_should_create_hooks(self):
        assert os.path.isfile(self._hook("post-checkout"))
        assert os.path.isfile(self._hook("pre-commit"))
        assert os.path.isfile(self._hook("pre-push"))

    def test_should_post_checkout_hook_checkout(self, repo_dir, dvc):
        stage_file = repo_dir.FOO + Stage.STAGE_FILE_SUFFIX

        dvc.add(repo_dir.FOO)
        dvc.scm.add([".gitignore", stage_file])
        dvc.scm.commit("add")

        os.unlink(repo_dir.FOO)
        dvc.scm.checkout("new_branc", create_new=True)

        assert os.path.isfile(repo_dir.FOO)

    def test_should_pre_push_hook_push(self, repo_dir, dvc):
        temp = repo_dir.mkdtemp()
        git_remote = os.path.join(temp, "project.git")
        storage_path = os.path.join(temp, "dvc_storage")

        foo_checksum = file_md5(repo_dir.FOO)[0]
        expected_cache_path = dvc.cache.local.get(foo_checksum)

        ret = main(["remote", "add", "-d", "store", storage_path])
        assert ret == 0

        ret = main(["add", repo_dir.FOO])
        assert ret == 0

        stage_file = repo_dir.FOO + Stage.STAGE_FILE_SUFFIX
        dvc.scm.git.index.add([stage_file, ".gitignore"])
        dvc.scm.git.index.commit("commit message")

        dvc.scm.git.clone(git_remote)
        dvc.scm.git.create_remote("origin", git_remote)

        dvc.scm.git.git.push("origin", "master")

        assert os.path.isfile(expected_cache_path)
