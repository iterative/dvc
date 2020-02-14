import logging
import os
import re
from datetime import datetime, timedelta
from urllib.parse import urlparse
import threading

from funcy import cached_property, wrap_prop

from dvc.config import Config
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.remote.base import RemoteBASE
from dvc.scheme import Schemes


logger = logging.getLogger(__name__)


class RemoteAZURE(RemoteBASE):
    scheme = Schemes.AZURE
    path_cls = CloudURLInfo
    REGEX = (
        r"azure://((?P<path>[^=;]*)?|("
        # backward compatibility
        r"(ContainerName=(?P<container_name>[^;]+);?)?"
        r"(?P<connection_string>.+)?)?)$"
    )
    REQUIRES = {"azure-storage-blob": "azure.storage.blob"}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get(Config.SECTION_REMOTE_URL, "azure://")

        match = re.match(self.REGEX, url)  # backward compatibility
        path = match.group("path")
        bucket = (
            urlparse(url if path else "").netloc
            or match.group("container_name")  # backward compatibility
            or os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        )

        self.connection_string = (
            config.get(Config.SECTION_AZURE_CONNECTION_STRING)
            or match.group("connection_string")  # backward compatibility
            or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )

        if not bucket:
            raise ValueError("azure storage container name missing")

        if not self.connection_string:
            raise ValueError("azure storage connection string missing")

        self.path_info = (
            self.path_cls(url)
            if path
            else self.path_cls.from_parts(scheme=self.scheme, netloc=bucket)
        )

    @wrap_prop(threading.Lock())
    @cached_property
    def blob_service(self):
        from azure.storage.blob import BlockBlobService
        from azure.common import AzureMissingResourceHttpError

        logger.debug("URL {}".format(self.path_info))
        logger.debug("Connection string {}".format(self.connection_string))
        blob_service = BlockBlobService(
            connection_string=self.connection_string
        )
        logger.debug("Container name {}".format(self.path_info.bucket))
        try:  # verify that container exists
            blob_service.list_blobs(
                self.path_info.bucket, delimiter="/", num_results=1
            )
        except AzureMissingResourceHttpError:
            blob_service.create_container(self.path_info.bucket)
        return blob_service

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug("Removing {}".format(path_info))
        self.blob_service.delete_blob(path_info.bucket, path_info.path)

    def _list_paths(self, bucket, prefix):
        blob_service = self.blob_service
        next_marker = None
        while True:
            blobs = blob_service.list_blobs(
                bucket, prefix=prefix, marker=next_marker
            )

            for blob in blobs:
                yield blob.name

            if not blobs.next_marker:
                break

            next_marker = blobs.next_marker

    def list_cache_paths(self):
        return self._list_paths(self.path_info.bucket, self.path_info.path)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.blob_service.create_blob_from_path(
                to_info.bucket,
                to_info.path,
                from_file,
                progress_callback=pbar.update_to,
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.blob_service.get_blob_to_path(
                from_info.bucket,
                from_info.path,
                to_file,
                progress_callback=pbar.update_to,
            )

    def exists(self, path_info):
        paths = self._list_paths(path_info.bucket, path_info.path)
        return any(path_info.path == path for path in paths)

    def _generate_download_url(self, path_info, expires=3600):
        from azure.storage.blob import BlobPermissions

        expires_at = datetime.utcnow() + timedelta(seconds=expires)

        sas_token = self.blob_service.generate_blob_shared_access_signature(
            path_info.bucket,
            path_info.path,
            permission=BlobPermissions.READ,
            expiry=expires_at,
        )
        download_url = self.blob_service.make_blob_url(
            path_info.bucket, path_info.path, sas_token=sas_token
        )
        return download_url
