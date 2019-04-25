import pytest
from git import Repo
from git.exc import GitCommandNotFound

from dvc.repo import Repo as DvcRepo
from .basic_env import TestDirFixture


@pytest.fixture(autouse=True)
def debug(caplog):
    with caplog.at_level("DEBUG", logger="dvc"):
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
