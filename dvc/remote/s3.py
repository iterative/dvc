from __future__ import unicode_literals

import os
import logging
import itertools
from funcy import cached_property

try:
    import boto3
except ImportError:
    boto3 = None

from dvc.progress import Tqdm
from dvc.config import Config
from dvc.remote.base import RemoteBASE
from dvc.exceptions import DvcException, ETagMismatchError
from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes

logger = logging.getLogger(__name__)


class RemoteS3(RemoteBASE):
    scheme = Schemes.S3
    path_cls = CloudURLInfo
    REQUIRES = {"boto3": boto3}
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super(RemoteS3, self).__init__(repo, config)

        storagepath = "s3://{}".format(
            config.get(Config.SECTION_AWS_STORAGEPATH, "").lstrip("/")
        )

        url = config.get(Config.SECTION_REMOTE_URL, storagepath)
        self.path_info = self.path_cls(url)

        self.region = config.get(Config.SECTION_AWS_REGION)

        self.profile = config.get(Config.SECTION_AWS_PROFILE)

        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL)

        self.list_objects = config.get(Config.SECTION_AWS_LIST_OBJECTS)

        self.use_ssl = config.get(Config.SECTION_AWS_USE_SSL, True)

        self.extra_args = {}

        self.sse = config.get(Config.SECTION_AWS_SSE, "")
        if self.sse:
            self.extra_args["ServerSideEncryption"] = self.sse

        shared_creds = config.get(Config.SECTION_AWS_CREDENTIALPATH)
        if shared_creds:
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

    @cached_property
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
    def _copy_multipart(
        cls, s3, from_info, to_info, size, n_parts, extra_args
    ):
        mpu = s3.create_multipart_upload(
            Bucket=to_info.bucket, Key=to_info.path, **extra_args
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
    def _copy(cls, s3, from_info, to_info, extra_args):
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
            cls._copy_multipart(
                s3, from_info, to_info, size, n_parts, extra_args=extra_args
            )
        else:
            source = {"Bucket": from_info.bucket, "Key": from_info.path}
            s3.copy(source, to_info.bucket, to_info.path, ExtraArgs=extra_args)

        cached_etag = cls.get_etag(s3, to_info.bucket, to_info.path)
        if etag != cached_etag:
            raise ETagMismatchError(etag, cached_etag)

    def copy(self, from_info, to_info):
        self._copy(self.s3, from_info, to_info, self.extra_args)

    def remove(self, path_info):
        if path_info.scheme != "s3":
            raise NotImplementedError

        logger.debug("Removing {}".format(path_info))
        self.s3.delete_object(Bucket=path_info.bucket, Key=path_info.path)

    def _list_paths(self, bucket, prefix):
        """ Read config for list object api, paginate through list objects."""
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if self.list_objects:
            list_objects_api = "list_objects"
        else:
            list_objects_api = "list_objects_v2"
        paginator = self.s3.get_paginator(list_objects_api)
        for page in paginator.paginate(**kwargs):
            contents = page.get("Contents", None)
            if not contents:
                continue
            for item in contents:
                yield item["Key"]

    def list_cache_paths(self):
        return self._list_paths(self.path_info.bucket, self.path_info.path)

    def exists(self, path_info):
        paths = self._list_paths(path_info.bucket, path_info.path)
        return any(path_info.path == path for path in paths)

    def batch_exists(self, path_infos, callback):
        paths = []

        for path_info in path_infos:
            paths.append(self._list_paths(path_info.bucket, path_info.path))
            callback.update(str(path_info))

        paths = set(itertools.chain.from_iterable(paths))
        return [path_info.path in paths for path_info in path_infos]

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        total = os.path.getsize(from_file)
        with Tqdm(disable=no_progress_bar, total=total, bytes=True,
                  desc_truncate=name) as pbar:
            self.s3.upload_file(
                from_file,
                to_info.bucket,
                to_info.path,
                Callback=pbar.update,
                ExtraArgs=self.extra_args,
            )

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        total = self.s3.head_object(
            Bucket=from_info.bucket, Key=from_info.path
        )["ContentLength"]
        with Tqdm(disable=no_progress_bar, total=total, bytes=True,
                  desc_truncate=name) as pbar:
            self.s3.download_file(
                from_info.bucket, from_info.path, to_file, Callback=pbar.update
            )
