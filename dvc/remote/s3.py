from __future__ import unicode_literals

import os
import threading
import logging

from dvc.path import Schemes
from dvc.path.s3 import S3PathInfo

try:
    import boto3
except ImportError:
    boto3 = None

from dvc.utils import tmp_fname, move
from dvc.utils.compat import urlparse, makedirs
from dvc.progress import progress
from dvc.config import Config
from dvc.remote.base import RemoteBase
from dvc.exceptions import DvcException, ETagMismatchError

logger = logging.getLogger(__name__)


class Callback(object):
    def __init__(self, name, total):
        self.name = name
        self.total = total
        self.current = 0
        self.lock = threading.Lock()

    def __call__(self, byts):
        with self.lock:
            self.current += byts
            progress.update_target(self.name, self.current, self.total)


class RemoteS3(RemoteBase):
    scheme = Schemes.S3
    REGEX = r"^s3://(?P<path>.*)$"
    REQUIRES = {"boto3": boto3}
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super(RemoteS3, self).__init__(repo, config)

        storagepath = "s3://{}".format(
            config.get(Config.SECTION_AWS_STORAGEPATH, "").lstrip("/")
        )

        self.url = config.get(Config.SECTION_REMOTE_URL, storagepath)

        self.region = os.environ.get("AWS_DEFAULT_REGION") or config.get(
            Config.SECTION_AWS_REGION
        )

        self.profile = os.environ.get("AWS_PROFILE") or config.get(
            Config.SECTION_AWS_PROFILE
        )

        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL)

        self.list_objects = config.get(Config.SECTION_AWS_LIST_OBJECTS)

        self.use_ssl = config.get(Config.SECTION_AWS_USE_SSL, True)

        shared_creds = config.get(Config.SECTION_AWS_CREDENTIALPATH)
        if shared_creds:
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

        parsed = urlparse(self.url)
        self.bucket = parsed.netloc
        self.prefix = parsed.path.lstrip("/")

        self.path_info = S3PathInfo(bucket=self.bucket)

    @staticmethod
    def compat_config(config):
        ret = config.copy()
        url = "s3://" + ret.pop(Config.SECTION_AWS_STORAGEPATH, "").lstrip("/")
        ret[Config.SECTION_REMOTE_URL] = url
        return ret

    @property
    def s3(self):
        session = boto3.session.Session(
            profile_name=self.profile, region_name=self.region
        )

        return session.client(
            "s3", endpoint_url=self.endpoint_url, use_ssl=self.use_ssl
        )

    @classmethod
    def get_etag(cls, s3, bucket, path):
        obj = cls.get_head_object(s3, bucket, path)

        return obj["ETag"].strip('"')

    def get_file_checksum(self, path_info):
        return self.get_etag(self.s3, path_info.bucket, path_info.path)

    @staticmethod
    def get_head_object(s3, bucket, path, *args, **kwargs):

        try:
            obj = s3.head_object(Bucket=bucket, Key=path, *args, **kwargs)
        except Exception as exc:
            raise DvcException(
                "s3://{}/{} does not exist".format(bucket, path), exc
            )
        return obj

    @classmethod
    def _copy_multipart(cls, s3, from_info, to_info, size, n_parts):
        mpu = s3.create_multipart_upload(
            Bucket=to_info.bucket, Key=to_info.path
        )
        mpu_id = mpu["UploadId"]

        parts = []
        byte_position = 0
        for i in range(1, n_parts + 1):
            obj = cls.get_head_object(
                s3, from_info.bucket, from_info.path, PartNumber=i
            )
            part_size = obj["ContentLength"]
            lastbyte = byte_position + part_size - 1
            if lastbyte > size:
                lastbyte = size - 1

            srange = "bytes={}-{}".format(byte_position, lastbyte)

            part = s3.upload_part_copy(
                Bucket=to_info.bucket,
                Key=to_info.path,
                PartNumber=i,
                UploadId=mpu_id,
                CopySourceRange=srange,
                CopySource={"Bucket": from_info.bucket, "Key": from_info.path},
            )
            parts.append(
                {"PartNumber": i, "ETag": part["CopyPartResult"]["ETag"]}
            )
            byte_position += part_size

        assert n_parts == len(parts)

        s3.complete_multipart_upload(
            Bucket=to_info.bucket,
            Key=to_info.path,
            UploadId=mpu_id,
            MultipartUpload={"Parts": parts},
        )

    @classmethod
    def _copy(cls, s3, from_info, to_info):
        # NOTE: object's etag depends on the way it was uploaded to s3 or the
        # way it was copied within the s3. More specifically, it depends on
        # the chunk size that was used to transfer it, which would affect
        # whether an object would be uploaded as a single part or as a
        # multipart.
        #
        # If an object's etag looks like '8978c98bb5a48c2fb5f2c4c905768afa',
        # then it was transfered as a single part, which means that the chunk
        # size used to transfer it was greater or equal to the ContentLength
        # of that object. So to preserve that tag over the next transfer, we
        # could use any value >= ContentLength.
        #
        # If an object's etag looks like '50d67013a5e1a4070bef1fc8eea4d5f9-13',
        # then it was transfered as a multipart, which means that the chunk
        # size used to transfer it was less than ContentLength of that object.
        # Unfortunately, in general, it doesn't mean that the chunk size was
        # the same throughout the transfer, so it means that in order to
        # preserve etag, we need to transfer each part separately, so the
        # object is transfered in the same chunks as it was originally.

        obj = cls.get_head_object(s3, from_info.bucket, from_info.path)
        etag = obj["ETag"].strip('"')
        size = obj["ContentLength"]

        _, _, parts_suffix = etag.partition("-")
        if parts_suffix:
            n_parts = int(parts_suffix)
            cls._copy_multipart(s3, from_info, to_info, size, n_parts)
        else:
            source = {"Bucket": from_info.bucket, "Key": from_info.path}
            s3.copy(source, to_info.bucket, to_info.path)

        cached_etag = cls.get_etag(s3, to_info.bucket, to_info.path)
        if etag != cached_etag:
            raise ETagMismatchError(etag, cached_etag)

    def copy(self, from_info, to_info, s3=None):
        s3 = s3 if s3 else self.s3
        self._copy(s3, from_info, to_info)

    def remove(self, path_info):
        if path_info.scheme != "s3":
            raise NotImplementedError

        logger.debug(
            "Removing s3://{}/{}".format(path_info.bucket, path_info.path)
        )

        self.s3.delete_object(Bucket=path_info.bucket, Key=path_info.path)

    def _list_paths(self, bucket, prefix):
        """ Read config for list object api, paginate through list objects."""
        s3 = self.s3
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if self.list_objects:
            list_objects_api = "list_objects"
        else:
            list_objects_api = "list_objects_v2"
        paginator = s3.get_paginator(list_objects_api)
        for page in paginator.paginate(**kwargs):
            contents = page.get("Contents", None)
            if not contents:
                continue
            for item in contents:
                yield item["Key"]

    def list_cache_paths(self):
        return self._list_paths(self.bucket, self.prefix)

    def exists(self, path_info):
        assert not isinstance(path_info, list)
        assert path_info.scheme == "s3"

        paths = self._list_paths(path_info.bucket, path_info.path)
        return any(path_info.path == path for path in paths)

    def upload(self, from_infos, to_infos, names=None, no_progress_bar=False):
        names = self._verify_path_args(to_infos, from_infos, names)

        s3 = self.s3

        for from_info, to_info, name in zip(from_infos, to_infos, names):
            if to_info.scheme != "s3":
                raise NotImplementedError

            if from_info.scheme != "local":
                raise NotImplementedError

            logger.debug(
                "Uploading '{}' to '{}/{}'".format(
                    from_info.path, to_info.bucket, to_info.path
                )
            )

            if not name:
                name = os.path.basename(from_info.path)

            total = os.path.getsize(from_info.path)
            cb = None if no_progress_bar else Callback(name, total)

            try:
                s3.upload_file(
                    from_info.path, to_info.bucket, to_info.path, Callback=cb
                )
            except Exception:
                msg = "failed to upload '{}'".format(from_info.path)
                logger.exception(msg)
                continue

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

        s3 = self.s3

        for to_info, from_info, name in zip(to_infos, from_infos, names):
            if from_info.scheme != "s3":
                raise NotImplementedError

            if to_info.scheme == "s3":
                self.copy(from_info, to_info, s3=s3)
                continue

            if to_info.scheme != "local":
                raise NotImplementedError

            msg = "Downloading '{}/{}' to '{}'".format(
                from_info.bucket, from_info.path, to_info.path
            )
            logger.debug(msg)

            tmp_file = tmp_fname(to_info.path)
            if not name:
                name = os.path.basename(to_info.path)

            makedirs(os.path.dirname(to_info.path), exist_ok=True)

            try:
                if no_progress_bar:
                    cb = None
                else:
                    total = s3.head_object(
                        Bucket=from_info.bucket, Key=from_info.path
                    )["ContentLength"]
                    cb = Callback(name, total)

                s3.download_file(
                    from_info.bucket, from_info.path, tmp_file, Callback=cb
                )
            except Exception:
                msg = "failed to download '{}/{}'".format(
                    from_info.bucket, from_info.path
                )
                logger.exception(msg)
                continue

            move(tmp_file, to_info.path)

            if not no_progress_bar:
                progress.finish_target(name)
