import mock
from dvc.remote.gdrive import RemoteGDrive


@mock.patch("dvc.remote.gdrive.RemoteGDrive.init_drive")
def test_init(repo):
    url = "gdrive://root/data"
    gdrive = RemoteGDrive(repo, {"url": url})
    assert str(gdrive.path_info) == url
