import pytest

from dvc.path_info import PathInfo
from dvc.remote.azure import AzureRemote
from tests.remotes import Azure

container_name = "container-name"
connection_string = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def test_init_env_var(monkeypatch, dvc):
    monkeypatch.setenv("AZURE_STORAGE_CONTAINER_NAME", container_name)
    monkeypatch.setenv("AZURE_STORAGE_CONNECTION_STRING", connection_string)

    config = {"url": "azure://"}
    remote = AzureRemote(dvc, config)
    assert remote.path_info == "azure://" + container_name
    assert remote.connection_string == connection_string


def test_init(dvc):
    prefix = "some/prefix"
    url = f"azure://{container_name}/{prefix}"
    config = {"url": url, "connection_string": connection_string}
    remote = AzureRemote(dvc, config)
    assert remote.path_info == url
    assert remote.connection_string == connection_string


def test_get_file_checksum(tmp_dir):
    if not Azure.should_test():
        pytest.skip("no azurite running")

    tmp_dir.gen("foo", "foo")

    remote = AzureRemote(None, {})
    to_info = remote.path_cls(Azure.get_url())
    remote.upload(PathInfo("foo"), to_info)
    assert remote.exists(to_info)
    checksum = remote.get_file_checksum(to_info)
    assert checksum
    assert isinstance(checksum, str)
    assert checksum.strip("'").strip('"') == checksum
