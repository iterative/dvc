import posixpath
from contextlib import contextmanager
from functools import partial

import pytest
from dvc_ssh.tests.cloud import TEST_SSH_KEY_PATH, TEST_SSH_USER

from dvc.fs.ssh import SSHFileSystem
from dvc.repo.experiments.base import EXEC_HEAD, EXEC_MERGE
from dvc.repo.experiments.executor.ssh import SSHExecutor
from tests.func.machine.conftest import *  # noqa, pylint: disable=wildcard-import


@contextmanager
def _ssh_factory(cloud):
    yield SSHFileSystem(
        host=cloud.host,
        port=cloud.port,
        user=TEST_SSH_USER,
        keyfile=TEST_SSH_KEY_PATH,
    )


def test_init_from_stash(tmp_dir, scm, dvc, machine_instance, mocker):
    mock = mocker.patch.object(SSHExecutor, "_from_stash_entry")
    mock_entry = mocker.Mock()
    mock_entry.name = ""
    SSHExecutor.from_stash_entry(
        None,
        "",
        mock_entry,
        manager=dvc.machine,
        machine_name="foo",
    )
    _args, kwargs = mock.call_args
    assert kwargs["host"] == machine_instance["instance_ip"]


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

    root_url = cloud / SSHExecutor.gen_dirname()

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


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("git_ssh")])
def test_init_cache(tmp_dir, dvc, scm, cloud):
    foo = tmp_dir.dvc_gen("foo", "foo", commit="init")[0].outs[0]
    rev = scm.get_rev()
    scm.set_ref(EXEC_HEAD, rev)
    scm.set_ref(EXEC_MERGE, rev)
    root_url = cloud / SSHExecutor.gen_dirname()

    executor = SSHExecutor(
        scm,
        ".",
        root_dir=root_url.path,
        host=root_url.host,
        port=root_url.port,
        username=TEST_SSH_USER,
        fs_factory=partial(_ssh_factory, cloud),
    )
    executor.init_cache(dvc, rev)

    fs = cloud._ssh
    foo_hash = foo.hash_info.value
    assert fs.exists(
        posixpath.join(
            executor._repo_abspath, ".dvc", "cache", foo_hash[:2], foo_hash[2:]
        )
    )
