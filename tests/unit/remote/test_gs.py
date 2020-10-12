import mock
import pytest
import requests

from dvc.tree.gs import GSTree, dynamic_chunk_size

BUCKET = "bucket"
PREFIX = "prefix"
URL = f"gs://{BUCKET}/{PREFIX}"
CREDENTIALPATH = "/path/to/gcp_credentials.json"
PROJECT = "PROJECT"
CONFIG = {
    "projectname": PROJECT,
    "url": URL,
    "credentialpath": CREDENTIALPATH,
}


def test_init(dvc):
    tree = GSTree(dvc, CONFIG)
    assert tree.path_info == URL
    assert tree.projectname == PROJECT
    assert tree.credentialpath == CREDENTIALPATH


@mock.patch("google.cloud.storage.Client.from_service_account_json")
def test_gs(mock_client, dvc):
    tree = GSTree(dvc, CONFIG)
    assert tree.credentialpath
    assert tree.gs
    mock_client.assert_called_once_with(CREDENTIALPATH)


@mock.patch("google.cloud.storage.Client")
def test_gs_no_credspath(mock_client, dvc):
    config = CONFIG.copy()
    del config["credentialpath"]
    tree = GSTree(dvc, config)
    assert tree.gs
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
