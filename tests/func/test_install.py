import os
import sys

import pytest
from dvc.utils import file_md5

from dvc.main import main
from dvc.stage import Stage


@pytest.mark.skipif(
    sys.platform == "win32", reason="Git hooks aren't supported on Windows"
)
class TestInstall(object):
    def _hook(self, name):
        return os.path.join(".git", "hooks", name)

    def test_should_create_hooks(self, dvc):
        assert main(["install"]) == 0

        hooks_with_commands = [
            ("post-checkout", "exec dvc checkout"),
            ("pre-commit", "exec dvc status"),
            ("pre-push", "exec dvc push"),
        ]

        for fname, command in hooks_with_commands:
            assert os.path.isfile(self._hook(fname))

            with open(self._hook(fname), "r") as fobj:
                assert command in fobj.read()

    def test_should_append_hooks_if_file_already_exists(self, dvc):
        with open(self._hook("post-checkout"), "w") as fobj:
            fobj.write("#!/bin/sh\n" "echo hello\n")

        assert main(["install"]) == 0

        expected_script = "#!/bin/sh\n" "echo hello\n" "exec dvc checkout\n"

        with open(self._hook("post-checkout"), "r") as fobj:
            assert fobj.read() == expected_script

    def test_should_be_idempotent(self, dvc):
        assert main(["install"]) == 0
        assert main(["install"]) == 0

        expected_script = "#!/bin/sh\n" "exec dvc checkout\n"

        with open(self._hook("post-checkout"), "r") as fobj:
            assert fobj.read() == expected_script

    def test_should_post_checkout_hook_checkout(self, repo_dir, dvc):
        assert main(["install"]) == 0

        stage_file = repo_dir.FOO + Stage.STAGE_FILE_SUFFIX

        dvc.add(repo_dir.FOO)
        dvc.scm.add([".gitignore", stage_file])
        dvc.scm.commit("add")

        os.unlink(repo_dir.FOO)
        dvc.scm.checkout("new_branc", create_new=True)

        assert os.path.isfile(repo_dir.FOO)

    def test_should_pre_push_hook_push(self, repo_dir, dvc):
        assert main(["install"]) == 0

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
