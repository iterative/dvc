import pytest

from dvc.remote.gdrive import RemoteGDrive


@pytest.fixture
def gdrive(repo):
    ret = RemoteGDrive(None, {"url": "gdrive://root/data"})
    return ret
