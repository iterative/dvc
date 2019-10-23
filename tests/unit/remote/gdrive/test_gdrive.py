import mock
from dvc.remote.gdrive import RemoteGDrive


@mock.patch("dvc.remote.gdrive.RemoteGDrive.init_drive")
def test_init_drive(repo):
    url = "gdrive://root/data"
    gdrive = RemoteGDrive(repo, {"url": url})
    assert str(gdrive.path_info) == url


@mock.patch("dvc.remote.gdrive.RemoteGDrive.init_drive")
def test_init_folder_id(repo):
    url = "gdrive://folder_id/data"
    gdrive = RemoteGDrive(repo, {"url": url})
    assert str(gdrive.path_info) == url
