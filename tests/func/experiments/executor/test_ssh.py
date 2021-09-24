import posixpath
from contextlib import contextmanager
from functools import partial

import pytest

from dvc.fs.ssh import SSHFileSystem
from dvc.path_info import URLInfo
from dvc.repo.experiments.base import EXEC_HEAD, EXEC_MERGE
from dvc.repo.experiments.executor.ssh import SSHExecutor
from tests.remotes.ssh import TEST_SSH_KEY_PATH, TEST_SSH_USER


@contextmanager
def _ssh_factory(cloud):
    yield SSHFileSystem(
        host=cloud.host,
        port=cloud.port,
        user=TEST_SSH_USER,
        keyfile=TEST_SSH_KEY_PATH,
    )


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("git_ssh")])
def test_init_git(tmp_dir, scm, cloud):
    tmp_dir.scm_gen({"foo": "foo", "dir": {"bar": "bar"}}, commit="init")
    rev = scm.get_rev()
    scm.set_ref(EXEC_HEAD, rev)
    tmp_dir.gen("foo", "stashed")
    scm.gitpython.git.stash()
    rev = scm.resolve_rev("stash@{0}")
    scm.set_ref(EXEC_MERGE, rev)

    root_url = URLInfo(str(cloud)) / SSHExecutor.gen_dirname()

    executor = SSHExecutor(
        scm,
        ".",
        root_dir=root_url.path,
        host=root_url.host,
        port=root_url.port,
        username=TEST_SSH_USER,
        fs_factory=partial(_ssh_factory, cloud),
    )
    assert root_url.path == executor._repo_abspath

    fs = cloud._ssh
    assert fs.exists(posixpath.join(executor._repo_abspath, "foo"))
    assert fs.exists(posixpath.join(executor._repo_abspath, "dir"))
    assert fs.exists(posixpath.join(executor._repo_abspath, "dir", "bar"))
