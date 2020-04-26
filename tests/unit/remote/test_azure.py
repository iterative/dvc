from dvc.remote.azure import AzureRemote


container_name = "container-name"
connection_string = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


def test_init_compat(dvc):
    url = (
        "azure://ContainerName={container_name};{connection_string}"
    ).format(
        container_name=container_name, connection_string=connection_string,
    )
    config = {"url": url}
    remote = AzureRemote(dvc, config)
    assert remote.path_info == "azure://" + container_name
    assert remote.connection_string == connection_string


def test_init(dvc):
    prefix = "some/prefix"
    url = "azure://{}/{}".format(container_name, prefix)
    config = {"url": url, "connection_string": connection_string}
    remote = AzureRemote(dvc, config)
    assert remote.path_info == url
    assert remote.connection_string == connection_string
