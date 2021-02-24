import logging
import os
import shutil
import threading

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.utils import format_link

from .base import BaseFileSystem

logger = logging.getLogger(__name__)
_DEFAULT_CREDS_STEPS = (
    "https://azuresdkdocs.blob.core.windows.net/$web/python/"
    "azure-identity/1.4.0/azure.identity.html#azure.identity"
    ".DefaultAzureCredential"
)


class AzureAuthError(DvcException):
    pass


class AzureFileSystem(BaseFileSystem):
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

        self.login_method, self.login_info = self._prepare_credentials(config)

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

        return login_method, login_info

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
            file_system = AzureBlobFileSystem(**self.login_info)
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

    def _with_bucket(self, path):
        if isinstance(path, self.PATH_CLS):
            return f"{path.bucket}/{path.path}"
        return path

    def open(
        self, path_info, mode="r", **kwargs
    ):  # pylint: disable=arguments-differ
        return self.fs.open(self._with_bucket(path_info), mode=mode)

    def exists(self, path_info, use_dvcignore=False):
        return self.fs.exists(self._with_bucket(path_info))

    def _strip_bucket(self, entry):
        _, entry = entry.split("/", 1)
        return entry

    def _strip_buckets(self, entries, detail, prefix=None):
        for entry in entries:
            if detail:
                entry = entry.copy()
                entry["name"] = self._strip_bucket(entry["name"])
            else:
                entry = self._strip_bucket(
                    f"{prefix}/{entry}" if prefix else entry
                )
            yield entry

    def ls(
        self, path_info, detail=False, recursive=False
    ):  # pylint: disable=arguments-differ
        path = self._with_bucket(path_info)
        if recursive:
            for root, _, files in self.fs.walk(path, detail=detail):
                if detail:
                    files = files.values()
                yield from self._strip_buckets(files, detail, prefix=root)
            return None

        yield from self._strip_buckets(self.ls(path, detail=detail), detail)

    def walk_files(self, path_info, **kwargs):
        for file in self.ls(path_info, recursive=True):
            yield path_info.replace(path=file)

    def remove(self, path_info):
        self.fs.delete(self._with_bucket(path_info))

    def info(self, path_info):
        info = self.fs.info(self._with_bucket(path_info)).copy()
        info["name"] = self._strip_bucket(info["name"])
        return info

    def _upload_fobj(self, fobj, to_info):
        from adlfs import AzureBlobFile

        with self.open(to_info, "wb") as fdest:
            shutil.copyfileobj(
                fobj, fdest, length=AzureBlobFile.DEFAULT_BLOCK_SIZE
            )

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **kwargs
    ):
        total = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                self._with_bucket(to_info),
                desc=name,
                total=total,
                no_progress_bar=no_progress_bar,
            )
        self.fs.invalidate_cache(self._with_bucket(to_info.parent))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **pbar_args
    ):
        total = self.getsize(self._with_bucket(from_info))
        with self.open(from_info, "rb") as fobj:
            with Tqdm.wrapattr(
                fobj,
                "read",
                desc=name,
                disable=no_progress_bar,
                bytes=True,
                total=total,
                **pbar_args,
            ) as wrapped:
                with open(to_file, "wb") as fdest:
                    shutil.copyfileobj(wrapped, fdest)
