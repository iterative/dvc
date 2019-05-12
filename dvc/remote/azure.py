from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import logging

from dvc.scheme import Schemes
from dvc.path.azure import PathAZURE

try:
    from azure.storage.blob import BlockBlobService
    from azure.common import AzureMissingResourceHttpError
except ImportError:
    BlockBlobService = None

from dvc.utils import tmp_fname, move
from dvc.utils.compat import urlparse, makedirs
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBASE


logger = logging.getLogger(__name__)


class Callback(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, current, total):
        progress.update_target(self.name, current, total)


class RemoteAZURE(RemoteBASE):
    scheme = Schemes.AZURE
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

        self.url = config.get(Config.SECTION_REMOTE_URL, "azure://")
        match = re.match(self.REGEX, self.url)  # backward compatibility

        path = match.group("path")
        self.bucket = (
            urlparse(self.url if path else "").netloc
            or match.group("container_name")  # backward compatibility
            or os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        )

        self.prefix = urlparse(self.url).path.lstrip("/") if path else ""

        self.connection_string = (
            config.get(Config.SECTION_AZURE_CONNECTION_STRING)
            or match.group("connection_string")  # backward compatibility
            or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        )

        if not self.bucket:
            raise ValueError("azure storage container name missing")

        if not self.connection_string:
            raise ValueError("azure storage connection string missing")

        self.__blob_service = None
        self.path_info = PathAZURE(bucket=self.bucket)

    @property
    def blob_service(self):
        if self.__blob_service is None:
            logger.debug("URL {}".format(self.url))
            logger.debug("Connection string {}".format(self.connection_string))
            self.__blob_service = BlockBlobService(
                connection_string=self.connection_string
            )
            logger.debug("Container name {}".format(self.bucket))
            try:  # verify that container exists
                self.__blob_service.list_blobs(
                    self.bucket, delimiter="/", num_results=1
                )
            except AzureMissingResourceHttpError:
                self.__blob_service.create_container(self.bucket)
        return self.__blob_service

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug(
            "Removing azure://{}/{}".format(path_info.bucket, path_info.path)
        )

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
        return self._list_paths(self.bucket, self.prefix)

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info.scheme != self.scheme:
                raise NotImplementedError

            if from_info.scheme != "local":
                raise NotImplementedError

            bucket = to_info.bucket
            path = to_info.path

            logger.debug(
                "Uploading '{}' to '{}/{}'".format(
                    from_info.path, bucket, path
                )
            )

            if not name:
                name = os.path.basename(from_info.path)

            cb = None if no_progress_bar else Callback(name)

            try:
                self.blob_service.create_blob_from_path(
                    bucket, path, from_info.path, progress_callback=cb
                )
            except Exception:
                msg = "failed to upload '{}'".format(from_info.path)
                logger.warning(msg)
            else:
                progress.finish_target(name)

    def download(
        self,
        from_infos,
        to_infos,
        no_progress_bar=False,
        names=None,
        resume=False,
    ):
        names = self._verify_path_args(from_infos, to_infos, names)

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != self.scheme:
                raise NotImplementedError

            if to_info.scheme != "local":
                raise NotImplementedError

            bucket = from_info.bucket
            path = from_info.path

            logger.debug(
                "Downloading '{}/{}' to '{}'".format(
                    bucket, path, to_info.path
                )
            )

            tmp_file = tmp_fname(to_info.path)
            if not name:
                name = os.path.basename(to_info.path)

            cb = None if no_progress_bar else Callback(name)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)

            try:
                self.blob_service.get_blob_to_path(
                    bucket, path, tmp_file, progress_callback=cb
                )
            except Exception:
                msg = "failed to download '{}/{}'".format(bucket, path)
                logger.warning(msg)
            else:
                move(tmp_file, to_info.path)

                if not no_progress_bar:
                    progress.finish_target(name)
