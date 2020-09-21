import logging
import os
import threading
from datetime import datetime, timedelta

from funcy import cached_property, wrap_prop

from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseTree

logger = logging.getLogger(__name__)


class AzureTree(BaseTree):
    scheme = Schemes.AZURE
    PATH_CLS = CloudURLInfo
    REQUIRES = {
        "azure-storage-blob": "azure.storage.blob",
        "knack": "knack",
    }
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5
    LIST_OBJECT_PAGE_SIZE = 5000

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url", "azure://")
        self.path_info = self.PATH_CLS(url)

        if not self.path_info.bucket:
            container = self._az_config.get("storage", "container_name", None)
            self.path_info = self.PATH_CLS(f"azure://{container}")

        self._conn_str = config.get(
            "connection_string"
        ) or self._az_config.get("storage", "connection_string", None)

        self._account_url = None
        if not self._conn_str:
            name = self._az_config.get("storage", "account", None)
            self._account_url = f"https://{name}.blob.core.windows.net"

        self._credential = config.get("sas_token") or self._az_config.get(
            "storage", "sas_token", None
        )
        if not self._credential:
            self._credential = self._az_config.get("storage", "key", None)

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
    def blob_service(self):
        # pylint: disable=no-name-in-module
        from azure.core.exceptions import (
            HttpResponseError,
            ResourceNotFoundError,
        )
        from azure.storage.blob import BlobServiceClient

        logger.debug(f"URL {self.path_info}")

        if self._conn_str:
            logger.debug(f"Using connection string '{self._conn_str}'")
            blob_service = BlobServiceClient.from_connection_string(
                self._conn_str, credential=self._credential
            )
        else:
            logger.debug(f"Using account url '{self._account_url}'")
            blob_service = BlobServiceClient(
                self._account_url, credential=self._credential
            )

        logger.debug(f"Container name {self.path_info.bucket}")
        container_client = blob_service.get_container_client(
            self.path_info.bucket
        )

        try:  # verify that container exists
            container_client.get_container_properties()
        except ResourceNotFoundError:
            container_client.create_container()
        except HttpResponseError as exc:
            # client may not have account-level privileges
            if exc.status_code != 403:
                raise

        return blob_service

    def get_etag(self, path_info):
        blob_client = self.blob_service.get_blob_client(
            path_info.bucket, path_info.path
        )
        etag = blob_client.get_blob_properties().etag
        return etag.strip('"')

    def _generate_download_url(self, path_info, expires=3600):
        from azure.storage.blob import (  # pylint:disable=no-name-in-module
            BlobSasPermissions,
            generate_blob_sas,
        )

        expires_at = datetime.utcnow() + timedelta(seconds=expires)

        blob_client = self.blob_service.get_blob_client(
            path_info.bucket, path_info.path
        )

        sas_token = generate_blob_sas(
            blob_client.account_name,
            blob_client.container_name,
            blob_client.blob_name,
            account_key=blob_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=expires_at,
        )
        return blob_client.url + "?" + sas_token

    def exists(self, path_info, use_dvcignore=True):
        paths = self._list_paths(path_info.bucket, path_info.path)
        return any(path_info.path == path for path in paths)

    def _list_paths(self, bucket, prefix):
        container_client = self.blob_service.get_container_client(bucket)
        for blob in container_client.list_blobs(name_starts_with=prefix):
            yield blob.name

    def walk_files(self, path_info, **kwargs):
        if not kwargs.pop("prefix", False):
            path_info = path_info / ""
        for fname in self._list_paths(
            path_info.bucket, path_info.path, **kwargs
        ):
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug(f"Removing {path_info}")
        self.blob_service.get_blob_client(
            path_info.bucket, path_info.path
        ).delete_blob()

    def get_file_hash(self, path_info):
        return HashInfo(self.PARAM_CHECKSUM, self.get_etag(path_info))

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):

        blob_client = self.blob_service.get_blob_client(
            to_info.bucket, to_info.path
        )
        total = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            with Tqdm.wrapattr(
                fobj, "read", desc=name, total=total, disable=no_progress_bar
            ) as wrapped:
                blob_client.upload_blob(wrapped, overwrite=True)

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        blob_client = self.blob_service.get_blob_client(
            from_info.bucket, from_info.path
        )
        total = blob_client.get_blob_properties().size
        stream = blob_client.download_blob()
        with open(to_file, "wb") as fobj:
            with Tqdm.wrapattr(
                fobj, "write", desc=name, total=total, disable=no_progress_bar
            ) as wrapped:
                stream.readinto(wrapped)
