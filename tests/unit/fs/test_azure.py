import pytest
from azure.identity import ChainedTokenCredential

from dvc.tree.azure import AzureTree

container_name = "test_container"
url = f"azure://{container_name}"
sas_token = "test_sas_token"
client_id = "test_id"
client_secret = "test_client_secret"
tenant_id = "test_tenant_id"
dummy_sas_token = "foo"
dummy_storage_account_key = "bar"

only_url_config = {"url": url}

sas_token_config = {"sas_token": dummy_sas_token}
account_key_config = {"sas_token": dummy_storage_account_key}

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


def test_if_sas_token_in_config_sas_token_is_credential(dvc):
    tree = AzureTree(dvc, sas_token_config)

    assert tree._credential == dummy_sas_token


def test_if_sas_token_in_env_sas_token_is_credential(
    monkeypatch, dvc, configure_env
):
    configure_env(monkeypatch, {"AZURE_STORAGE_SAS_TOKEN": dummy_sas_token})
    tree = AzureTree(dvc, only_url_config)

    assert tree._credential == dummy_sas_token


def test_if_account_key_in_config_account_key_is_credential(dvc):
    tree = AzureTree(dvc, account_key_config)

    assert tree._credential == dummy_storage_account_key


def test_if_account_key_in_env_account_key_is_credential(
    monkeypatch, dvc, configure_env
):
    configure_env(
        monkeypatch, {"AZURE_STORAGE_KEY": dummy_storage_account_key}
    )
    tree = AzureTree(dvc, only_url_config)

    assert tree._credential == dummy_storage_account_key


def test_credential_is_chained_token_credential_if_no_auth_string_tokens(dvc):
    # Here auth string tokens are
    # - storage account key
    # - SAS token

    svc_config = {
        "url": url,
    }
    tree = AzureTree(dvc, svc_config)
    assert isinstance(tree._credential, ChainedTokenCredential)
    assert tree._conn_str is None
