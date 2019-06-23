from __future__ import unicode_literals
import mockssh
import pytest
import os
from git import Repo
from git.exc import GitCommandNotFound

from dvc.remote.config import RemoteConfig
from dvc.utils.compat import cast_bytes_py2
from dvc.remote.ssh.connection import SSHConnection
from dvc.repo import Repo as DvcRepo
from .basic_env import TestDirFixture, TestDvcGitFixture


# Prevent updater and analytics from running their processes
os.environ[cast_bytes_py2("DVC_TEST")] = cast_bytes_py2("true")


@pytest.fixture(autouse=True)
def reset_loglevel(request, caplog):
    """
    Use it to ensure log level at the start of each test
    regardless of dvc.logger.setup(), Repo configs or whatever.
    """
    level = request.config.getoption("--log-level")
    if level:
        with caplog.at_level(level.upper(), logger="dvc"):
            yield
    else:
        yield


# Wrap class like fixture as pytest-like one to avoid code duplication
@pytest.fixture
def repo_dir():
    old_fixture = TestDirFixture()
    old_fixture.setUp()
    try:
        yield old_fixture
    finally:
        old_fixture.tearDown()


# NOTE: this duplicates code from GitFixture,
# would fix itself once class-based fixtures are removed
@pytest.fixture
def git(repo_dir):
    # NOTE: handles EAGAIN error on BSD systems (osx in our case).
    # Otherwise when running tests you might get this exception:
    #
    #    GitCommandNotFound: Cmd('git') not found due to:
    #        OSError('[Errno 35] Resource temporarily unavailable')
    retries = 5
    while True:
        try:
            git = Repo.init()
            break
        except GitCommandNotFound:
            retries -= 1
            if not retries:
                raise

    try:
        git.index.add([repo_dir.CODE])
        git.index.commit("add code")
        yield git
    finally:
        git.close()


@pytest.fixture
def dvc_repo(repo_dir):
    yield DvcRepo.init(repo_dir._root_dir, no_scm=True)


here = os.path.abspath(os.path.dirname(__file__))

user = "user"
key_path = os.path.join(here, "{0}.key".format(user))


@pytest.fixture
def ssh_server():
    users = {user: key_path}
    with mockssh.Server(users) as s:
        yield s


@pytest.fixture
def ssh(ssh_server):
    yield SSHConnection(
        ssh_server.host,
        username=user,
        port=ssh_server.port,
        key_filename=key_path,
    )


@pytest.fixture
def temporary_windows_drive(repo_dir):
    import string
    import win32api
    from ctypes import windll
    from win32con import DDD_REMOVE_DEFINITION

    drives = [
        s[0].upper()
        for s in win32api.GetLogicalDriveStrings().split("\000")
        if len(s) > 0
    ]

    new_drive_name = [
        letter for letter in string.ascii_uppercase if letter not in drives
    ][0]
    new_drive = "{}:".format(new_drive_name)

    target_path = repo_dir.mkdtemp()

    set_up_result = windll.kernel32.DefineDosDeviceW(0, new_drive, target_path)
    if set_up_result == 0:
        raise RuntimeError("Failed to mount windows drive!")

    # NOTE: new_drive has form of `A:` and joining it with some relative
    # path might result in non-existing path (A:path\\to)
    yield os.path.join(new_drive, os.sep)

    tear_down_result = windll.kernel32.DefineDosDeviceW(
        DDD_REMOVE_DEFINITION, new_drive, target_path
    )
    if tear_down_result == 0:
        raise RuntimeError("Could not unmount windows drive!")


@pytest.fixture
def erepo(repo_dir):
    repo = TestDvcGitFixture()
    repo.setUp()
    try:
        stage_foo = repo.dvc.add(repo.FOO)[0]
        stage_bar = repo.dvc.add(repo.BAR)[0]
        stage_data_dir = repo.dvc.add(repo.DATA_DIR)[0]
        repo.dvc.scm.add([stage_foo.path, stage_bar.path, stage_data_dir.path])
        repo.dvc.scm.commit("init repo")

        rconfig = RemoteConfig(repo.dvc.config)
        rconfig.add("upstream", repo.dvc.cache.local.cache_dir, default=True)
        repo.dvc.scm.add([repo.dvc.config.config_file])
        repo.dvc.scm.commit("add remote")

        repo.create("version", "master")
        repo.dvc.add("version")
        repo.dvc.scm.add([".gitignore", "version.dvc"])
        repo.dvc.scm.commit("master")

        repo.dvc.scm.checkout("branch", create_new=True)
        os.unlink(os.path.join(repo.root_dir, "version"))
        repo.create("version", "branch")
        repo.dvc.add("version")
        repo.dvc.scm.add([".gitignore", "version.dvc"])
        repo.dvc.scm.commit("branch")

        repo.dvc.scm.checkout("master")

        repo.dvc.scm.git.close()
        repo.git.close()

        os.chdir(repo._saved_dir)
        yield repo
    finally:
        repo.tearDown()
