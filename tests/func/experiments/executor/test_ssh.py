import posixpath
from contextlib import contextmanager
from functools import partial
from urllib.parse import urlparse

import pytest
from dvc_ssh import SSHFileSystem
from dvc_ssh.tests.cloud import TEST_SSH_KEY_PATH, TEST_SSH_USER

from dvc.repo.experiments.executor.base import ExecutorInfo, ExecutorResult
from dvc.repo.experiments.executor.ssh import SSHExecutor
from dvc.repo.experiments.refs import EXEC_HEAD, EXEC_MERGE
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
        dvc,
        mock_entry,
        machine_name="foo",
    )
    _args, kwargs = mock.call_args
    assert kwargs["host"] == machine_instance["instance_ip"]


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("git_ssh")])
def test_init_git(tmp_dir, dvc, scm, cloud, mocker):
    tmp_dir.scm_gen({"foo": "foo", "dir": {"bar": "bar"}}, commit="init")
    baseline_rev = scm.get_rev()
    tmp_dir.gen("foo", "stashed")
    scm.gitpython.git.stash()
    rev = scm.resolve_rev("stash@{0}")

    mock = mocker.Mock(baseline_rev=baseline_rev, head_rev=baseline_rev)

    root_url = cloud / SSHExecutor.gen_dirname()

    executor = SSHExecutor(
        root_dir=root_url.path,
        dvc_dir=".dvc",
        baseline_rev=baseline_rev,
        host=root_url.host,
        port=root_url.port,
        username=TEST_SSH_USER,
        fs_factory=partial(_ssh_factory, cloud),
    )
    infofile = str((root_url / "foo.run").path)
    executor.init_git(dvc, scm, rev, mock, infofile=infofile)
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
        root_dir=root_url.path,
        dvc_dir=".dvc",
        baseline_rev=rev,
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


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("git_ssh")])
def test_reproduce(tmp_dir, scm, dvc, cloud, exp_stage, mocker):
    from sshfs import SSHFileSystem as _sshfs  # noqa: N813

    rev = scm.get_rev()
    root_url = cloud / SSHExecutor.gen_dirname()
    mocker.patch.object(SSHFileSystem, "exists", return_value=True)
    mock_execute = mocker.patch.object(_sshfs, "execute")
    info = ExecutorInfo(
        str(root_url),
        rev,
        "machine-foo",
        str(root_url.path),
        ".dvc",
    )
    infofile = str((root_url / "foo.run").path)
    SSHExecutor.reproduce(
        info,
        rev,
        fs_factory=partial(_ssh_factory, cloud),
    )
    mock_execute.assert_called_once()
    _name, args, _kwargs = mock_execute.mock_calls[0]
    assert f"dvc exp exec-run --infofile {infofile}" in args[0]


@pytest.mark.needs_internet
@pytest.mark.parametrize("cloud", [pytest.lazy_fixture("git_ssh")])
def test_run_machine(tmp_dir, scm, dvc, cloud, exp_stage, mocker):
    baseline = scm.get_rev()
    factory = partial(_ssh_factory, cloud)
    mocker.patch.object(
        dvc.machine,
        "get_executor_kwargs",
        return_value={
            "host": cloud.host,
            "port": cloud.port,
            "username": TEST_SSH_USER,
            "fs_factory": factory,
        },
    )
    mocker.patch.object(dvc.machine, "get_setup_script", return_value=None)
    mock_repro = mocker.patch.object(
        SSHExecutor,
        "reproduce",
        return_value=ExecutorResult("abc123", None, False),
    )

    tmp_dir.gen("params.yaml", "foo: 2")
    dvc.experiments.run(exp_stage.addressing, machine="foo")
    mock_repro.assert_called_once()
    _name, _args, kwargs = mock_repro.mock_calls[0]
    info = kwargs["info"]
    url = urlparse(info.git_url)
    assert url.scheme == "ssh"
    assert url.hostname == cloud.host
    assert url.port == cloud.port
    assert info.baseline_rev == baseline
    assert kwargs["infofile"] is not None
    assert kwargs["fs_factory"] is not None
