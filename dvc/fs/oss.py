import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes

from .base import BaseFileSystem

logger = logging.getLogger(__name__)


class OSSFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
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
    PATH_CLS = CloudURLInfo
    REQUIRES = {"oss2": "oss2"}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5
    LIST_OBJECT_PAGE_SIZE = 100

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url")
        self.path_info = self.PATH_CLS(url) if url else None

        self.bucket = self.path_info.bucket
        self.endpoint = config.get("oss_endpoint") or os.getenv("OSS_ENDPOINT")

        self.key_id = (
            config.get("oss_key_id")
            or os.getenv("OSS_ACCESS_KEY_ID")
            or "defaultId"
        )

        self.key_secret = (
            config.get("oss_key_secret")
            or os.getenv("OSS_ACCESS_KEY_SECRET")
            or "defaultSecret"
        )

    @wrap_prop(threading.Lock())
    @cached_property
    def oss_service(self):
        import oss2

        logger.debug(f"key id: {self.key_id}")
        logger.debug(f"key secret: {self.key_secret}")

        auth = oss2.Auth(self.key_id, self.key_secret)
        bucket = oss2.Bucket(auth, self.endpoint, self.bucket)

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

    def _generate_download_url(self, path_info, expires=3600):
        assert path_info.bucket == self.bucket

        return self.oss_service.sign_url("GET", path_info.path, expires)

    def exists(self, path_info, use_dvcignore=True):
        paths = self._list_paths(path_info)
        return any(path_info.path == path for path in paths)

    def _list_paths(self, path_info):
        import oss2

        for blob in oss2.ObjectIterator(
            self.oss_service, prefix=path_info.path
        ):
            yield blob.key

    def walk_files(self, path_info, **kwargs):
        if not kwargs.pop("prefix", False):
            path_info = path_info / ""
        for fname in self._list_paths(path_info):
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def remove(self, path_info):
        if path_info.scheme != self.scheme:
            raise NotImplementedError

        logger.debug(f"Removing oss://{path_info}")
        self.oss_service.delete_object(path_info.path)

    def _upload_fobj(self, fobj, to_info):
        self.oss_service.put_object(to_info.path, fobj)

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
            import oss2

            oss2.resumable_download(
                self.oss_service,
                from_info.path,
                to_file,
                progress_callback=pbar.update_to,
            )
