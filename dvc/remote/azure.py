from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import logging

from dvc.scheme import Schemes

try:
    from azure.storage.blob import BlockBlobService
    from azure.common import AzureMissingResourceHttpError
except ImportError:
    BlockBlobService = None

from dvc.utils.compat import urlparse
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBASE
from dvc.path_info import CloudURLInfo


logger = logging.getLogger(__name__)


class Callback(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, current, total):
        progress.update_target(self.name, current, total)


class RemoteAZURE(RemoteBASE):
    scheme = Schemes.AZURE
    path_cls = CloudURLInfo
    REGEX = (
        r"azure://((?P<path>[^=;]*)?|("
        # backward compatibility
        r"(ContainerName=(?P<container_name>[^;]+);?)?"
        r"(?P<connection_string>.+)?)?)$"
    )
    REQUIRES = {"azure-storage-blob": BlockBlobService}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5

    def __init__(self, repo, config):
        super(RemoteAZURE, self).__init__(repo, config)

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

        self.__blob_service = None
        self.path_info = (
            self.path_cls(url)
            if path
            else self.path_cls.from_parts(scheme=self.scheme, netloc=bucket)
        )

    @property
    def blob_service(self):
        if self.__blob_service is None:
            logger.debug("URL {}".format(self.path_info))
            logger.debug("Connection string {}".format(self.connection_string))
            self.__blob_service = BlockBlobService(
                connection_string=self.connection_string
            )
            logger.debug("Container name {}".format(self.path_info.bucket))
            try:  # verify that container exists
                self.__blob_service.list_blobs(
                    self.path_info.bucket, delimiter="/", num_results=1
                )
            except AzureMissingResourceHttpError:
                self.__blob_service.create_container(self.path_info.bucket)
        return self.__blob_service

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
        cb = None if no_progress_bar else Callback(name)
        self.blob_service.create_blob_from_path(
            to_info.bucket, to_info.path, from_file, progress_callback=cb
        )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        cb = None if no_progress_bar else Callback(name)
        self.blob_service.get_blob_to_path(
            from_info.bucket, from_info.path, to_file, progress_callback=cb
        )
