from __future__ import absolute_import
from __future__ import unicode_literals

import os
import logging

from dvc.scheme import Schemes

try:
    import oss2
except ImportError:
    oss2 = None

from dvc.config import Config
from dvc.remote.base import RemoteBASE
from dvc.progress import Tqdm
from dvc.path_info import CloudURLInfo


logger = logging.getLogger(__name__)


class RemoteOSS(RemoteBASE):
    """
    oss2 document:
    https://www.alibabacloud.com/help/doc-detail/32026.htm


    Examples
    ----------
    $ dvc remote add myremote oss://my-bucket/path
    Set key id, key secret and endpoint using modify command
    $ dvc remote modify myremote oss_key_id my-key-id
    $ dvc remote modify myremote oss_key_secret my-key-secret
    $ dvc remote modify myremote oss_endpoint endpoint
    or environment variables
    $ export OSS_ACCESS_KEY_ID="my-key-id"
    $ export OSS_ACCESS_KEY_SECRET="my-key-secret"
    $ export OSS_ENDPOINT="endpoint"
    """

    scheme = Schemes.OSS
    path_cls = CloudURLInfo
    REQUIRES = {"oss2": oss2}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5

    def __init__(self, repo, config):
        super(RemoteOSS, self).__init__(repo, config)

        url = config.get(Config.SECTION_REMOTE_URL)
        self.path_info = self.path_cls(url) if url else None

        self.endpoint = config.get(Config.SECTION_OSS_ENDPOINT) or os.getenv(
            "OSS_ENDPOINT"
        )

        self.key_id = (
            config.get(Config.SECTION_OSS_ACCESS_KEY_ID)
            or os.getenv("OSS_ACCESS_KEY_ID")
            or "defaultId"
        )

        self.key_secret = (
            config.get(Config.SECTION_OSS_ACCESS_KEY_SECRET)
            or os.getenv("OSS_ACCESS_KEY_SECRET")
            or "defaultSecret"
        )

        self._bucket = None

    @property
    def oss_service(self):
        if self._bucket is None:
            logger.debug("URL {}".format(self.path_info))
            logger.debug("key id {}".format(self.key_id))
            logger.debug("key secret {}".format(self.key_secret))
            auth = oss2.Auth(self.key_id, self.key_secret)
            self._bucket = oss2.Bucket(
                auth, self.endpoint, self.path_info.bucket
            )
            try:  # verify that bucket exists
                self._bucket.get_bucket_info()
            except oss2.exceptions.NoSuchBucket:
                self._bucket.create_bucket(
                    oss2.BUCKET_ACL_PUBLIC_READ,
                    oss2.models.BucketCreateConfig(
                        oss2.BUCKET_STORAGE_CLASS_STANDARD
                    ),
                )
        return self._bucket

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug("Removing oss://{}".format(path_info))
        self.oss_service.delete_object(path_info.path)

    def _list_paths(self, prefix):
        for blob in oss2.ObjectIterator(self.oss_service, prefix=prefix):
            yield blob.key

    def list_cache_paths(self):
        return self._list_paths(self.path_info.path)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc_truncate=name, disable=no_progress_bar) as pbar:
            self.oss_service.put_object_from_file(
                to_info.path, from_file, progress_callback=pbar.update_to
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc_truncate=name, disable=no_progress_bar) as pbar:
            self.oss_service.get_object_to_file(
                from_info.path, to_file, progress_callback=pbar.update_to
            )
