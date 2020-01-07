import os
import pathlib
import sys

import pytest

from dvc.exceptions import GitHookAlreadyExistsError
from dvc.remote import RemoteConfig
from dvc.utils import file_md5, fspath


@pytest.mark.skipif(
    sys.platform == "win32", reason="Git hooks aren't supported on Windows"
)
class TestInstall(object):
    def _hook(self, name):
        return pathlib.Path(".git") / "hooks" / name

    def test_create_hooks(self, scm, dvc):
        scm.install()

        hooks_with_commands = [
            ("post-checkout", "exec dvc checkout"),
            ("pre-commit", "exec dvc status"),
            ("pre-push", "exec dvc push"),
        ]

        for fname, command in hooks_with_commands:
            hook_path = self._hook(fname)
            assert hook_path.is_file()
            assert command in hook_path.read_text()

    def test_fail_if_hook_exists(self, scm):
        self._hook("post-checkout").write_text("hook content")

        with pytest.raises(GitHookAlreadyExistsError):
            scm.install()

    def test_post_checkout(self, tmp_dir, scm, dvc):
        scm.install()
        tmp_dir.dvc_gen({"file": "file content"}, commit="add")

        os.unlink("file")
        scm.checkout("new_branch", create_new=True)

        assert os.path.isfile("file")

    def test_pre_push_hook(self, tmp_dir, scm, dvc, tmp_path_factory):
        scm.install()

        temp = tmp_path_factory.mktemp("external")
        git_remote = temp / "project.git"
        storage_path = temp / "dvc_storage"

        RemoteConfig(dvc.config).add(
            "store", fspath(storage_path), default=True
        )
        tmp_dir.dvc_gen("file", "file_content", "commit message")

        file_checksum = file_md5("file")[0]
        expected_storage_path = (
            storage_path / file_checksum[:2] / file_checksum[2:]
        )

        scm.repo.clone(fspath(git_remote))
        scm.repo.create_remote("origin", fspath(git_remote))

        assert not expected_storage_path.is_file()
        scm.repo.git.push("origin", "master")
        assert expected_storage_path.is_file()
        assert expected_storage_path.read_text() == "file_content"
