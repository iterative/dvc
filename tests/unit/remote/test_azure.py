from dvc.fs.azure import AzureFileSystem
from dvc.path_info import PathInfo

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
    fs = AzureFileSystem(dvc, config)
    assert fs.path_info == "azure://" + container_name


def test_init(dvc):
    prefix = "some/prefix"
    url = f"azure://{container_name}/{prefix}"
    config = {"url": url, "connection_string": connection_string}
    fs = AzureFileSystem(dvc, config)
    assert fs.path_info == url


def test_info(tmp_dir, azure):
    tmp_dir.gen("foo", "foo")

    fs = AzureFileSystem(None, azure.config)
    to_info = azure
    fs.upload(PathInfo("foo"), to_info)
    assert fs.exists(to_info)
    hash_ = fs.info(to_info)["etag"]
    assert isinstance(hash_, str)
    assert hash_.strip("'").strip('"') == hash_
