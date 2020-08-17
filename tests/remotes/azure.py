# pylint:disable=abstract-method

import uuid

import pytest

from dvc.path_info import CloudURLInfo

from .base import Base

TEST_AZURE_CONTAINER = "tests"
TEST_AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSR"
    "Z6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://127.0.0.1:{port}/devstoreaccount1;"
)


class Azure(Base, CloudURLInfo):
    pass


@pytest.fixture(scope="session")
def azure_server(docker_compose, docker_services):
    from azure.core.exceptions import (  # pylint: disable=no-name-in-module
        AzureError,
    )
    from azure.storage.blob import (  # pylint: disable=no-name-in-module
        BlobServiceClient,
    )

    port = docker_services.port_for("azurite", 10000)
    connection_string = TEST_AZURE_CONNECTION_STRING.format(port=port)

    def _check():
        try:
            BlobServiceClient.from_connection_string(
                connection_string
            ).list_containers()
            return True
        except AzureError:
            return False

    docker_services.wait_until_responsive(
        timeout=60.0, pause=0.1, check=_check
    )

    return connection_string


@pytest.fixture
def azure(azure_server):
    url = f"azure://{TEST_AZURE_CONTAINER}/{uuid.uuid4()}"
    ret = Azure(url)
    ret.config = {
        "url": url,
        "connection_string": azure_server,
    }
    return ret
