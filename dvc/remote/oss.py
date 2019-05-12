from __future__ import absolute_import
from __future__ import unicode_literals

import os
import logging

from dvc.scheme import Schemes
from dvc.path.oss import PathOSS

try:
    import oss2
except ImportError:
    oss2 = None

from dvc.utils import tmp_fname, move
from dvc.utils.compat import urlparse, makedirs
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBASE
from dvc.remote.azure import Callback


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
    REGEX = r"^oss://(?P<path>.*)?$"
    REQUIRES = {"oss2": oss2}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5

    def __init__(self, repo, config):
        super(RemoteOSS, self).__init__(repo, config)

        self.url = config.get(Config.SECTION_REMOTE_URL)
        parsed = urlparse(self.url)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.lstrip("/")

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
        self.path_info = PathOSS(bucket=self.bucket)

    @property
    def oss_service(self):
        if self._bucket is None:
            logger.debug("URL {}".format(self.url))
            logger.debug("key id {}".format(self.key_id))
            logger.debug("key secret {}".format(self.key_secret))
            auth = oss2.Auth(self.key_id, self.key_secret)
            logger.debug("bucket name {}".format(self.bucket))
            self._bucket = oss2.Bucket(auth, self.endpoint, self.bucket)
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

        logger.debug(
            "Removing oss://{}/{}".format(path_info.bucket, path_info.path)
        )

        self.oss_service.delete_object(path_info.path)

    def _list_paths(self, prefix):
        for blob in oss2.ObjectIterator(self.oss_service, prefix=prefix):
            yield blob.key

    def list_cache_paths(self):
        return self._list_paths(self.prefix)

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
                "Uploading '{}' to 'oss://{}/{}'".format(
                    from_info.path, bucket, path
                )
            )

            if not name:
                name = os.path.basename(from_info.path)

            cb = None if no_progress_bar else Callback(name)

            try:
                self.oss_service.put_object_from_file(
                    path, from_info.path, progress_callback=cb
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
        names=None,
        no_progress_bar=False,
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
                "Downloading 'oss://{}/{}' to '{}'".format(
                    bucket, path, to_info.path
                )
            )

            tmp_file = tmp_fname(to_info.path)
            if not name:
                name = os.path.basename(to_info.path)

            cb = None if no_progress_bar else Callback(name)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)

            try:
                self.oss_service.get_object_to_file(
                    path, tmp_file, progress_callback=cb
                )
            except Exception:
                msg = "failed to download 'oss://{}/{}'".format(bucket, path)
                logger.warning(msg)
            else:
                move(tmp_file, to_info.path)

                if not no_progress_bar:
                    progress.finish_target(name)
