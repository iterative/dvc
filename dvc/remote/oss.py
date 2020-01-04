import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.config import Config
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.remote.base import RemoteBASE
from dvc.scheme import Schemes


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
    REQUIRES = {"oss2": "oss2"}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5

    def __init__(self, repo, config):
        super().__init__(repo, config)

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

    @wrap_prop(threading.Lock())
    @cached_property
    def oss_service(self):
        import oss2

        logger.debug("URL: {}".format(self.path_info))
        logger.debug("key id: {}".format(self.key_id))
        logger.debug("key secret: {}".format(self.key_secret))

        auth = oss2.Auth(self.key_id, self.key_secret)
        bucket = oss2.Bucket(auth, self.endpoint, self.path_info.bucket)

        # Ensure bucket exists
        try:
            bucket.get_bucket_info()
        except oss2.exceptions.NoSuchBucket:
            bucket.create_bucket(
                oss2.BUCKET_ACL_PUBLIC_READ,
                oss2.models.BucketCreateConfig(
                    oss2.BUCKET_STORAGE_CLASS_STANDARD
                ),
            )
        return bucket

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug("Removing oss://{}".format(path_info))
        self.oss_service.delete_object(path_info.path)

    def _list_paths(self, prefix):
        import oss2

        for blob in oss2.ObjectIterator(self.oss_service, prefix=prefix):
            yield blob.key

    def list_cache_paths(self):
        return self._list_paths(self.path_info.path)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.oss_service.put_object_from_file(
                to_info.path, from_file, progress_callback=pbar.update_to
            )

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **_kwargs
    ):
        with Tqdm(desc=name, disable=no_progress_bar, bytes=True) as pbar:
            self.oss_service.get_object_to_file(
                from_info.path, to_file, progress_callback=pbar.update_to
            )

    def _generate_download_url(self, path_info, expires=3600):
        assert path_info.bucket == self.path_info.bucket

        return self.oss_service.sign_url("GET", path_info.path, expires)

    def exists(self, path_info):
        paths = self._list_paths(path_info.path)
        return any(path_info.path == path for path in paths)
