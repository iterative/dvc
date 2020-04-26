import mock
import pytest
import requests

from dvc.remote.gs import dynamic_chunk_size
from dvc.remote.gs import GSRemote


BUCKET = "bucket"
PREFIX = "prefix"
URL = "gs://{}/{}".format(BUCKET, PREFIX)
CREDENTIALPATH = "/path/to/gcp_credentials.json"
PROJECT = "PROJECT"
CONFIG = {
    "projectname": PROJECT,
    "url": URL,
    "credentialpath": CREDENTIALPATH,
}


def test_init(dvc):
    remote = GSRemote(dvc, CONFIG)
    assert remote.path_info == URL
    assert remote.projectname == PROJECT
    assert remote.credentialpath == CREDENTIALPATH


@mock.patch("google.cloud.storage.Client.from_service_account_json")
def test_gs(mock_client, dvc):
    remote = GSRemote(dvc, CONFIG)
    assert remote.credentialpath
    remote.gs()
    mock_client.assert_called_once_with(CREDENTIALPATH)


@mock.patch("google.cloud.storage.Client")
def test_gs_no_credspath(mock_client, dvc):
    config = CONFIG.copy()
    del config["credentialpath"]
    remote = GSRemote(dvc, config)
    remote.gs()
    mock_client.assert_called_with(PROJECT)


def test_dynamic_chunk_size():
    chunk_sizes = []

    @dynamic_chunk_size
    def upload(chunk_size=None):
        chunk_sizes.append(chunk_size)
        raise requests.exceptions.ConnectionError()

    with pytest.raises(requests.exceptions.ConnectionError):
        upload()

    assert chunk_sizes == [10485760, 5242880, 2621440, 1310720, 524288, 262144]
