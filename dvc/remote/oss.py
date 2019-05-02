from __future__ import absolute_import
from __future__ import unicode_literals

import os
import re
import string
import logging

try:
    import oss2
except ImportError:
    oss2 = None

from dvc.utils import tmp_fname, move
from dvc.utils.compat import urlparse, makedirs
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase


logger = logging.getLogger(__name__)


class Callback(object):
    def __init__(self, name):
        self.name = name

    def __call__(self, current, total):
        progress.update_target(self.name, current, total)


class RemoteOSS(RemoteBase):
    """
    oss2 document:
    https://www.alibabacloud.com/help/doc-detail/32026.htm


    Examples
    ----------
    $ dvc remote add myremote oss://my-bucket.endpoint/path
    Set key id and key secret using modify command
    $ dvc remote modify myremote oss_key_id my-key-id
    $ dvc remote modify myremote oss_key_secret my-key-secret
    or environment variables
    $ export OSS_ACCESS_KEY_ID="my-key-id"
    $ export OSS_ACCESS_KEY_SECRET="my-key-secret"
    """

    scheme = "oss"
    REGEX = (
        r"^oss://(?P<url>(?P<bucket>.*?)\.(?P<endpoint>.*?))?(?P<path>/.*)?$"
    )
    REQUIRES = {"oss2": oss2}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5

    def __init__(self, repo, config):
        super(RemoteOSS, self).__init__(repo, config)

        self.url = config.get(Config.SECTION_REMOTE_URL)
        match = re.match(self.REGEX, self.url)  # backward compatibility

        self.bucket = match.group("bucket") or os.getenv("OSS_BUCKET")
        if not self.bucket:
            raise ValueError("oss bucket name is missing")
        self.bucket = self.bucket.lower()
        valid_chars = set(string.ascii_lowercase) | set(string.digits) | {"-"}
        if (
            len(set(self.bucket) - valid_chars) != 0
            or self.bucket[0] == "-"
            or self.bucket[-1] == "-"
            or len(self.bucket) < 3
            or len(self.bucket) > 63
        ):
            raise ValueError(
                "oss bucket name should only contrains lowercase "
                "alphabet letters, digits and - with length "
                "between 3 and 63"
            )

        self.endpoint = match.group("endpoint") or os.getenv("OSS_ENDPOINT")
        if not self.endpoint:
            raise ValueError("oss endpoint is missing")

        path = match.group("path")
        self.prefix = urlparse(self.url).path.lstrip("/") if path else ""

        self.key_id = config.get(
            Config.SECTION_OSS_ACCESS_KEY_ID
        ) or os.getenv("OSS_ACCESS_KEY_ID")
        if not self.key_id:
            raise ValueError("oss access key id is missing")

        self.key_secret = config.get(
            Config.SECTION_OSS_ACCESS_KEY_SECRET
        ) or os.getenv("OSS_ACCESS_KEY_SECRET")
        if not self.key_secret:
            raise ValueError("oss access key secret is missing")

        self._bucket = None
        self.path_info = {
            "scheme": self.scheme,
            "bucket": self.bucket,
            "endpoint": self.endpoint,
        }

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
        if path_info["scheme"] != self.scheme:
            raise NotImplementedError

        logger.debug(
            "Removing oss://{}.{}/{}".format(
                path_info["bucket"], path_info["endpoint"], path_info["path"]
            )
        )

        self.oss_service.delete_object(path_info["path"])

    def _list_paths(self, prefix):
        for blob in oss2.ObjectIterator(self.oss_service, prefix=prefix):
            yield blob.key

    def list_cache_paths(self):
        return self._list_paths(self.prefix)

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info["scheme"] != self.scheme:
                raise NotImplementedError

            if from_info["scheme"] != "local":
                raise NotImplementedError

            bucket = to_info["bucket"]
            path = to_info["path"]
            endpoint = to_info["endpoint"]

            logger.debug(
                "Uploading '{}' to 'oss://{}.{}/{}'".format(
                    from_info["path"], bucket, endpoint, path
                )
            )

            if not name:
                name = os.path.basename(from_info["path"])

            cb = None if no_progress_bar else Callback(name)

            try:
                self.oss_service.put_object_from_file(
                    path, from_info["path"], progress_callback=cb
                )
            except Exception:
                msg = "failed to upload '{}'".format(from_info["path"])
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
            if from_info["scheme"] != self.scheme:
                raise NotImplementedError

            if to_info["scheme"] != "local":
                raise NotImplementedError

            bucket = from_info["bucket"]
            path = from_info["path"]
            endpoint = from_info["endpoint"]

            logger.debug(
                "Downloading 'oss://{}.{}/{}' to '{}'".format(
                    bucket, endpoint, path, to_info["path"]
                )
            )

            tmp_file = tmp_fname(to_info["path"])
            if not name:
                name = os.path.basename(to_info["path"])

            cb = None if no_progress_bar else Callback(name)

            makedirs(os.path.dirname(to_info["path"]), exist_ok=True)

            try:
                self.oss_service.get_object_to_file(
                    path, tmp_file, progress_callback=cb
                )
            except Exception:
                msg = "failed to download 'oss://{}.{}/{}'".format(
                    bucket, endpoint, path
                )
                logger.warning(msg)
            else:
                move(tmp_file, to_info["path"])

                if not no_progress_bar:
                    progress.finish_target(name)
