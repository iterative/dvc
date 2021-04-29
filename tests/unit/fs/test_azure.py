import asyncio
from concurrent.futures import ThreadPoolExecutor

import pytest

from dvc.fs.azure import AzureFileSystem, _temp_event_loop
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
    fs = AzureFileSystem(**config)
    assert fs.path_info == "azure://" + container_name


def test_init(dvc):
    prefix = "some/prefix"
    url = f"azure://{container_name}/{prefix}"
    config = {"url": url, "connection_string": connection_string}
    fs = AzureFileSystem(**config)
    assert fs.path_info == url


def test_info(tmp_dir, azure):
    tmp_dir.gen("foo", "foo")

    fs = AzureFileSystem(**azure.config)
    to_info = azure
    fs.upload(PathInfo("foo"), to_info)
    assert fs.exists(to_info)
    hash_ = fs.info(to_info)["etag"]
    assert isinstance(hash_, str)
    assert hash_.strip("'").strip('"') == hash_


def test_temp_event_loop():
    def procedure():
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.sleep(0))
        return "yeey"

    def wrapped_procedure():
        with _temp_event_loop():
            return procedure()

        # it should clean the loop after
        # exitting the context.
        with pytest.raises(RuntimeError):
            asyncio.get_event_loop()

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(procedure)

        with pytest.raises(RuntimeError):
            future.result()

        future = executor.submit(wrapped_procedure)
        assert future.result() == "yeey"
