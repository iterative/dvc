import pytest
from azure.identity import ChainedTokenCredential, ClientSecretCredential

from dvc.tree.azure import AzureTree

container_name = "test_container"
url = f"azure://{container_name}"
client_id = "test_id"
client_secret = "test_client_secret"
tenant_id = "test_tenant_id"

only_url_config = {"url": url}

config = {
    "url": url,
    "client_id": client_id,
    "client_secret": client_secret,
    "tenant_id": tenant_id,
}

env_vars = {
    "AZURE_CLIENT_ID": client_id,
    "AZURE_CLIENT_SECRET": client_secret,
    "AZURE_TENANT_ID": tenant_id,
}


@pytest.fixture(scope="function")
def configure_env():
    def _configure_env(monkeypatch, var_dict):
        for var, val in var_dict.items():
            monkeypatch.setenv(var, val)

    return _configure_env


def test_init(dvc):
    tree = AzureTree(dvc, config)
    assert tree.path_info == url


def test_none_credential_if_not_all_svc_principle_details_in_config(dvc):
    incomplete_config = config.copy()
    incomplete_config.pop("tenant_id", None)
    tree = AzureTree(dvc, incomplete_config)

    assert tree._credential is None


# ClientSecretCredentials do not have a tenant property
# Enough to see exptected client id and secret percolated.
def test_credential_is_client_secret_credential(dvc):
    tree = AzureTree(dvc, config)
    assert isinstance(tree._credential, ClientSecretCredential)
    assert tree._credential._client_id == client_id
    assert tree._credential._secret == client_secret


def test_credential_is_client_secret_credential_from_env_vars(
    monkeypatch, dvc, configure_env
):
    configure_env(monkeypatch, env_vars)

    tree = AzureTree(dvc, only_url_config)

    assert isinstance(tree._credential, ClientSecretCredential)
    assert tree._credential._client_id == client_id
    assert tree._credential._secret == client_secret


def test_none_credential_if_not_all_svc_principle_details_in_env(
    monkeypatch, dvc, configure_env
):
    incomplete_env = env_vars.copy()
    incomplete_env.pop("AZURE_TENANT_ID", None)
    configure_env(monkeypatch, incomplete_env)
    tree = AzureTree(dvc, only_url_config)

    assert tree._credential is None


def test_credential_is_chained_token_credential(dvc):
    default_credential_config = {"url": url, "azcli_credential": True}
    tree = AzureTree(dvc, default_credential_config)
    assert isinstance(tree._credential, ChainedTokenCredential)
