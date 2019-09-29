import pytest

from dvc.repo import Repo
from dvc.remote.gdrive import RemoteGDrive


@pytest.fixture()
def repo():
    return Repo(".")


@pytest.fixture
def gdrive(repo):
    ret = RemoteGDrive(repo, {"url": "gdrive://root/data"})
    return ret
