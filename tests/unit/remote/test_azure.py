from dvc.path_info import PathInfo
from dvc.tree.azure import AzureTree

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
    tree = AzureTree(dvc, config)
    assert tree.path_info == "azure://" + container_name
    assert tree._conn_str == connection_string


def test_init(dvc):
    prefix = "some/prefix"
    url = f"azure://{container_name}/{prefix}"
    config = {"url": url, "connection_string": connection_string}
    tree = AzureTree(dvc, config)
    assert tree.path_info == url
    assert tree._conn_str == connection_string


def test_get_file_hash(tmp_dir, azure):
    tmp_dir.gen("foo", "foo")

    tree = AzureTree(None, azure.config)
    to_info = azure
    tree.upload(PathInfo("foo"), to_info)
    assert tree.exists(to_info)
    _, hash_ = tree.get_file_hash(to_info)
    assert hash_
    assert isinstance(hash_, str)
    assert hash_.strip("'").strip('"') == hash_
