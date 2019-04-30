from __future__ import unicode_literals
import mockssh
import pytest
import os
from git import Repo
from git.exc import GitCommandNotFound

from dvc.remote.ssh.connection import SSHConnection
from dvc.repo import Repo as DvcRepo
from .basic_env import TestDirFixture


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
    while retries:
        try:
            git = Repo.init()
        except GitCommandNotFound:
            retries -= 1
            continue
        break

    try:
        git.index.add([repo_dir.CODE])
        git.index.commit("add code")
        yield git
    finally:
        git.close()


@pytest.fixture
def dvc(repo_dir, git):
    try:
        dvc = DvcRepo.init(repo_dir._root_dir)
        dvc.scm.commit("init dvc")
        yield dvc
    finally:
        dvc.scm.git.close()


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
