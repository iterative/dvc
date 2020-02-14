# -*- coding: utf-8 -*-

import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.config import Config, ConfigError
from dvc.exceptions import DvcException
from dvc.exceptions import ETagMismatchError
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.remote.base import RemoteBASE
from dvc.scheme import Schemes

logger = logging.getLogger(__name__)


class RemoteS3(RemoteBASE):
    scheme = Schemes.S3
    path_cls = CloudURLInfo
    REQUIRES = {"boto3": "boto3"}
    PARAM_CHECKSUM = "etag"

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get(Config.SECTION_REMOTE_URL, "s3://")
        self.path_info = self.path_cls(url)

        self.region = config.get(Config.SECTION_AWS_REGION)

        self.profile = config.get(Config.SECTION_AWS_PROFILE)

        self.endpoint_url = config.get(Config.SECTION_AWS_ENDPOINT_URL)

        if config.get(Config.SECTION_AWS_LIST_OBJECTS):
            self.list_objects_api = "list_objects"
        else:
            self.list_objects_api = "list_objects_v2"

        self.use_ssl = config.get(Config.SECTION_AWS_USE_SSL, True)

        self.extra_args = {}

        self.sse = config.get(Config.SECTION_AWS_SSE, "")
        if self.sse:
            self.extra_args["ServerSideEncryption"] = self.sse

        self.acl = config.get(Config.SECTION_AWS_ACL, "")
        if self.acl:
            self.extra_args["ACL"] = self.acl

        self._append_aws_grants_to_extra_args(config)

        shared_creds = config.get(Config.SECTION_AWS_CREDENTIALPATH)
        if shared_creds:
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

    @wrap_prop(threading.Lock())
    @cached_property
    def s3(self):
        import boto3

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
                "s3://{}/{} does not exist".format(bucket, path)
            ) from exc
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
        # then it was transferred as a single part, which means that the chunk
        # size used to transfer it was greater or equal to the ContentLength
        # of that object. So to preserve that tag over the next transfer, we
        # could use any value >= ContentLength.
        #
        # If an object's etag looks like '50d67013a5e1a4070bef1fc8eea4d5f9-13',
        # then it was transferred as a multipart, which means that the chunk
        # size used to transfer it was less than ContentLength of that object.
        # Unfortunately, in general, it doesn't mean that the chunk size was
        # the same throughout the transfer, so it means that in order to
        # preserve etag, we need to transfer each part separately, so the
        # object is transfered in the same chunks as it was originally.
        from boto3.s3.transfer import TransferConfig

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
            s3.copy(
                source,
                to_info.bucket,
                to_info.path,
                ExtraArgs=extra_args,
                Config=TransferConfig(multipart_threshold=size + 1),
            )

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

    def _list_objects(self, path_info, max_items=None):
        """ Read config for list object api, paginate through list objects."""
        kwargs = {
            "Bucket": path_info.bucket,
            "Prefix": path_info.path,
            "PaginationConfig": {"MaxItems": max_items},
        }
        paginator = self.s3.get_paginator(self.list_objects_api)
        for page in paginator.paginate(**kwargs):
            yield from page.get("Contents", ())

    def _list_paths(self, path_info, max_items=None):
        return (
            item["Key"] for item in self._list_objects(path_info, max_items)
        )

    def list_cache_paths(self):
        return self._list_paths(self.path_info)

    def isfile(self, path_info):
        from botocore.exceptions import ClientError

        if path_info.path.endswith("/"):
            return False

        try:
            self.s3.head_object(Bucket=path_info.bucket, Key=path_info.path)
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "404":
                raise
            return False

        return True

    def exists(self, path_info):
        """Check if the blob exists. If it does not exist,
        it could be a part of a directory path.

        eg: if `data/file.txt` exists, check for `data` should return True
        """
        return self.isfile(path_info) or self.isdir(path_info)

    def makedirs(self, path_info):
        # We need to support creating empty directories, which means
        # creating an object with an empty body and a trailing slash `/`.
        #
        # We are not creating directory objects for every parent prefix,
        # as it is not required.
        dir_path = path_info / ""
        self.s3.put_object(Bucket=path_info.bucket, Key=dir_path.path, Body="")

    def isdir(self, path_info):
        # S3 doesn't have a concept for directories.
        #
        # Using `head_object` with a path pointing to a directory
        # will throw a 404 error.
        #
        # A reliable way to know if a given path is a directory is by
        # checking if there are more files sharing the same prefix
        # with a `list_objects` call.
        #
        # We need to make sure that the path ends with a forward slash,
        # since we can end with false-positives like the following example:
        #
        #       bucket
        #       └── data
        #          ├── alice
        #          └── alpha
        #
        # Using `data/al` as prefix will return `[data/alice, data/alpha]`,
        # While `data/al/` will return nothing.
        #
        dir_path = path_info / ""
        return bool(list(self._list_paths(dir_path, max_items=1)))

    def _upload(self, from_file, to_info, name=None, no_progress_bar=False):
        total = os.path.getsize(from_file)
        with Tqdm(
            disable=no_progress_bar, total=total, bytes=True, desc=name
        ) as pbar:
            self.s3.upload_file(
                from_file,
                to_info.bucket,
                to_info.path,
                Callback=pbar.update,
                ExtraArgs=self.extra_args,
            )

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        if no_progress_bar:
            total = None
        else:
            total = self.s3.head_object(
                Bucket=from_info.bucket, Key=from_info.path
            )["ContentLength"]
        with Tqdm(
            disable=no_progress_bar, total=total, bytes=True, desc=name
        ) as pbar:
            self.s3.download_file(
                from_info.bucket, from_info.path, to_file, Callback=pbar.update
            )

    def _generate_download_url(self, path_info, expires=3600):
        params = {"Bucket": path_info.bucket, "Key": path_info.path}
        return self.s3.generate_presigned_url(
            ClientMethod="get_object", Params=params, ExpiresIn=int(expires)
        )

    def walk_files(self, path_info, max_items=None):
        for fname in self._list_paths(path_info / "", max_items):
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def _append_aws_grants_to_extra_args(self, config):
        # Keys for extra_args can be one of the following list:
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_UPLOAD_ARGS
        """
          ALLOWED_UPLOAD_ARGS = [
            'ACL', 'CacheControl', 'ContentDisposition', 'ContentEncoding',
            'ContentLanguage', 'ContentType', 'Expires', 'GrantFullControl',
            'GrantRead', 'GrantReadACP', 'GrantWriteACP', 'Metadata',
            'RequestPayer', 'ServerSideEncryption', 'StorageClass',
            'SSECustomerAlgorithm', 'SSECustomerKey', 'SSECustomerKeyMD5',
            'SSEKMSKeyId', 'WebsiteRedirectLocation'
          ]
        """

        grants = {
            Config.SECTION_AWS_GRANT_FULL_CONTROL: "GrantFullControl",
            Config.SECTION_AWS_GRANT_READ: "GrantRead",
            Config.SECTION_AWS_GRANT_READ_ACP: "GrantReadACP",
            Config.SECTION_AWS_GRANT_WRITE_ACP: "GrantWriteACP",
        }

        for grant_option, extra_args_key in grants.items():
            if config.get(grant_option, ""):
                if self.acl:
                    raise ConfigError(
                        "`acl` and `grant_*` AWS S3 config options "
                        "are mutually exclusive"
                    )

                self.extra_args[extra_args_key] = config.get(grant_option)
