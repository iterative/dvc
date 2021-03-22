import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes
from dvc.utils import format_link

from .fsspec_wrapper import FSSpecWrapper

logger = logging.getLogger(__name__)
_DEFAULT_CREDS_STEPS = (
    "https://azuresdkdocs.blob.core.windows.net/$web/python/"
    "azure-identity/1.4.0/azure.identity.html#azure.identity"
    ".DefaultAzureCredential"
)


class AzureAuthError(DvcException):
    pass


class AzureFileSystem(FSSpecWrapper):
    scheme = Schemes.AZURE
    PATH_CLS = CloudURLInfo
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))
    REQUIRES = {
        "adlfs": "adlfs",
        "knack": "knack",
        "azure-identity": "azure.identity",
    }

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url")
        self.path_info = self.PATH_CLS(url)
        self.bucket = self.path_info.bucket

        if not self.bucket:
            container = self._az_config.get("storage", "container_name", None)
            url = f"azure://{container}"

        self.path_info = self.PATH_CLS(url)
        self.bucket = self.path_info.bucket

    def _prepare_credentials(self, config):
        from azure.identity.aio import DefaultAzureCredential

        login_info = {}
        login_info["connection_string"] = config.get(
            "connection_string",
            self._az_config.get("storage", "connection_string", None),
        )
        login_info["account_name"] = config.get(
            "account_name", self._az_config.get("storage", "account", None)
        )
        login_info["account_key"] = config.get(
            "account_key", self._az_config.get("storage", "key", None)
        )
        login_info["sas_token"] = config.get(
            "sas_token", self._az_config.get("storage", "sas_token", None)
        )
        login_info["tenant_id"] = config.get("tenant_id")
        login_info["client_id"] = config.get("client_id")
        login_info["client_secret"] = config.get("client_secret")

        if not any(login_info.values()):
            login_info["credentials"] = DefaultAzureCredential()

        for login_method, required_keys in [  # noqa
            ("connection string", ["connection_string"]),
            (
                "AD service principal",
                ["tenant_id", "client_id", "client_secret"],
            ),
            ("account key", ["account_name", "account_key"]),
            ("SAS token", ["account_name", "sas_token"]),
            ("anonymous login", ["account_name"]),
            (f"default credentials ({_DEFAULT_CREDS_STEPS})", ["credentials"]),
        ]:
            if all(login_info.get(key) is not None for key in required_keys):
                break
        else:
            login_method = None

        self.login_method = login_method
        return login_info

    @cached_property
    def _az_config(self):
        # NOTE: ideally we would've used get_default_cli().config from
        # azure.cli.core, but azure-cli-core has a lot of conflicts with other
        # dependencies. So instead we are just use knack directly
        from knack.config import CLIConfig

        config_dir = os.getenv(
            "AZURE_CONFIG_DIR", os.path.expanduser(os.path.join("~", ".azure"))
        )
        return CLIConfig(config_dir=config_dir, config_env_var_prefix="AZURE")

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from adlfs import AzureBlobFileSystem
        from azure.core.exceptions import AzureError

        try:
            file_system = AzureBlobFileSystem(**self.fs_args)
            if self.bucket not in [
                container.rstrip("/") for container in file_system.ls("/")
            ]:
                file_system.mkdir(self.bucket)
        except (ValueError, AzureError) as e:
            raise AzureAuthError(
                f"Authentication to Azure Blob Storage via {self.login_method}"
                " failed.\nLearn more about configuration settings at"
                f" {format_link('https://man.dvc.org/remote/modify')}"
            ) from e

        return file_system
