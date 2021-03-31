import logging
import os
import threading
from contextlib import contextmanager

from funcy import cached_property, wrap_prop

from dvc.config import ConfigError
from dvc.exceptions import DvcException, ETagMismatchError
from dvc.hash_info import HashInfo
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.utils import conversions, error_link

from .base import BaseFileSystem

logger = logging.getLogger(__name__)

_AWS_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".aws", "config")


class S3FileSystem(BaseFileSystem):
    scheme = Schemes.S3
    PATH_CLS = CloudURLInfo
    REQUIRES = {"boto3": "boto3"}
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url", "s3://")
        self.path_info = self.PATH_CLS(url)

        self.region = config.get("region")
        self.profile = config.get("profile")
        self.endpoint_url = config.get("endpointurl")

        self.use_ssl = config.get("use_ssl", True)
        self.ssl_verify = config.get("ssl_verify", True)

        self.extra_args = {}

        self.sse = config.get("sse")
        if self.sse:
            self.extra_args["ServerSideEncryption"] = self.sse

        self.sse_kms_key_id = config.get("sse_kms_key_id")
        if self.sse_kms_key_id:
            self.extra_args["SSEKMSKeyId"] = self.sse_kms_key_id

        self.acl = config.get("acl")
        if self.acl:
            self.extra_args["ACL"] = self.acl

        self._append_aws_grants_to_extra_args(config)

        self.access_key_id = config.get("access_key_id")
        self.secret_access_key = config.get("secret_access_key")
        self.session_token = config.get("session_token")

        shared_creds = config.get("credentialpath")
        if shared_creds:
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

        config_path = config.get("configpath")
        if config_path:
            os.environ.setdefault("AWS_CONFIG_FILE", config_path)
        self._transfer_config = None

    # https://github.com/aws/aws-cli/blob/0376c6262d6b15dc36c82e6da6e1aad10249cc8c/awscli/customizations/s3/transferconfig.py#L107-L113
    _TRANSFER_CONFIG_ALIASES = {
        "max_queue_size": "max_io_queue",
        "max_concurrent_requests": "max_concurrency",
        "multipart_threshold": "multipart_threshold",
        "multipart_chunksize": "multipart_chunksize",
    }

    def _transform_config(self, s3_config):
        """Splits the general s3 config into 2 different config
        objects, one for transfer.TransferConfig and other is the
        general session config"""

        config, transfer_config = {}, {}
        for key, value in s3_config.items():
            if key in self._TRANSFER_CONFIG_ALIASES:
                if key in {"multipart_chunksize", "multipart_threshold"}:
                    # cast human readable sizes (like 24MiB) to integers
                    value = conversions.human_readable_to_bytes(value)
                else:
                    value = int(value)
                transfer_config[self._TRANSFER_CONFIG_ALIASES[key]] = value
            else:
                config[key] = value

        return config, transfer_config

    def _process_config(self):
        from boto3.s3.transfer import TransferConfig
        from botocore.configloader import load_config

        config_path = os.environ.get("AWS_CONFIG_FILE", _AWS_CONFIG_PATH)
        if not os.path.exists(config_path):
            return None

        config = load_config(config_path)
        profile = config["profiles"].get(self.profile or "default")
        if not profile:
            return None

        s3_config = profile.get("s3", {})
        s3_config, transfer_config = self._transform_config(s3_config)
        self._transfer_config = TransferConfig(**transfer_config)
        return s3_config

    @wrap_prop(threading.Lock())
    @cached_property
    def s3(self):
        import boto3

        session_opts = {
            "profile_name": self.profile,
            "region_name": self.region,
        }

        if self.access_key_id:
            session_opts["aws_access_key_id"] = self.access_key_id
        if self.secret_access_key:
            session_opts["aws_secret_access_key"] = self.secret_access_key
        if self.session_token:
            session_opts["aws_session_token"] = self.session_token

        session = boto3.session.Session(**session_opts)
        s3_config = self._process_config()

        return session.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            verify=self.ssl_verify,
            use_ssl=self.use_ssl,
            config=boto3.session.Config(
                signature_version="s3v4", s3=s3_config
            ),
        )

    @contextmanager
    def _get_s3(self):
        from botocore.exceptions import (
            EndpointConnectionError,
            NoCredentialsError,
        )

        try:
            yield self.s3
        except NoCredentialsError as exc:
            link = error_link("no-credentials")
            raise DvcException(
                f"Unable to find AWS credentials. {link}"
            ) from exc
        except EndpointConnectionError as exc:
            link = error_link("connection-error")
            name = self.endpoint_url or "AWS S3"
            raise DvcException(
                f"Unable to connect to '{name}'. {link}"
            ) from exc

    @contextmanager
    def _get_bucket(self, bucket):
        with self._get_s3() as s3:
            try:
                yield s3.Bucket(bucket)
            except s3.meta.client.exceptions.NoSuchBucket as exc:
                link = error_link("no-bucket")
                raise DvcException(
                    f"Bucket '{bucket}' does not exist. {link}"
                ) from exc

    @contextmanager
    def _get_obj(self, path_info):
        with self._get_bucket(path_info.bucket) as bucket:
            try:
                yield bucket.Object(path_info.path)
            except bucket.meta.client.exceptions.NoSuchKey as exc:
                raise DvcException(f"{path_info.url} does not exist") from exc

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
            "grant_full_control": "GrantFullControl",
            "grant_read": "GrantRead",
            "grant_read_acp": "GrantReadACP",
            "grant_write_acp": "GrantWriteACP",
        }

        for grant_option, extra_args_key in grants.items():
            if config.get(grant_option):
                if self.acl:
                    raise ConfigError(
                        "`acl` and `grant_*` AWS S3 config options "
                        "are mutually exclusive"
                    )

                self.extra_args[extra_args_key] = config.get(grant_option)

    def _generate_download_url(self, path_info, expires=3600):
        params = {"Bucket": path_info.bucket, "Key": path_info.path}
        with self._get_s3() as s3:
            return s3.meta.client.generate_presigned_url(
                ClientMethod="get_object",
                Params=params,
                ExpiresIn=int(expires),
            )

    def exists(self, path_info, use_dvcignore=True):
        """Check if the blob exists. If it does not exist,
        it could be a part of a directory path.

        eg: if `data/file.txt` exists, check for `data` should return True
        """
        return self.isfile(path_info) or self.isdir(path_info)

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

    def isfile(self, path_info):
        if path_info.path.endswith("/"):
            return False

        return path_info.path in self._list_paths(path_info)

    def info(self, path_info):
        # temporary workaround, will be removed when migrating to s3fs
        if self.isdir(path_info):
            return {"type": "directory"}

        with self._get_obj(path_info) as obj:
            return {
                "type": "file",
                "etag": obj.e_tag.strip('"'),
                "size": obj.content_length,
            }

    def _list_paths(self, path_info, max_items=None):
        with self._get_bucket(path_info.bucket) as bucket:
            obj_summaries = bucket.objects.filter(Prefix=path_info.path)
            if max_items is not None:
                obj_summaries = obj_summaries.page_size(max_items).limit(
                    max_items
                )
            for obj_summary in obj_summaries:
                yield obj_summary.key

    def walk_files(self, path_info, **kwargs):
        if not kwargs.pop("prefix", False):
            path_info = path_info / ""
        for fname in self._list_paths(path_info, **kwargs):
            if fname.endswith("/"):
                continue

            yield path_info.replace(path=fname)

    def ls(
        self, path_info, detail=False, recursive=False
    ):  # pylint: disable=arguments-differ
        assert recursive

        with self._get_bucket(path_info.bucket) as bucket:
            for obj_summary in bucket.objects.filter(Prefix=path_info.path):
                if detail:
                    yield {
                        "type": "file",
                        "name": obj_summary.key,
                        "size": obj_summary.size,
                        "etag": obj_summary.e_tag.strip('"'),
                    }
                else:
                    yield obj_summary.key

    def remove(self, path_info):
        if path_info.scheme != "s3":
            raise NotImplementedError

        logger.debug(f"Removing {path_info}")
        with self._get_obj(path_info) as obj:
            obj.delete()

    def makedirs(self, path_info):
        # We need to support creating empty directories, which means
        # creating an object with an empty body and a trailing slash `/`.
        #
        # We are not creating directory objects for every parent prefix,
        # as it is not required.
        if not path_info.path:
            return

        dir_path = path_info / ""
        with self._get_obj(dir_path) as obj:
            obj.put(Body="")

    def copy(self, from_info, to_info):
        with self._get_s3() as s3:
            self._copy(s3.meta.client, from_info, to_info, self.extra_args)

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
            obj = s3.head_object(
                Bucket=from_info.bucket, Key=from_info.path, PartNumber=i
            )
            part_size = obj["ContentLength"]
            lastbyte = byte_position + part_size - 1
            if lastbyte > size:
                lastbyte = size - 1

            srange = f"bytes={byte_position}-{lastbyte}"

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

        obj = s3.head_object(Bucket=from_info.bucket, Key=from_info.path)
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

        cached_etag = s3.head_object(Bucket=to_info.bucket, Key=to_info.path)[
            "ETag"
        ].strip('"')
        if etag != cached_etag:
            raise ETagMismatchError(etag, cached_etag)

    def etag(self, path_info):
        with self._get_obj(path_info) as obj:
            return HashInfo("etag", size=obj.content_length,)

    def _upload_fobj(self, fobj, to_info):
        with self._get_obj(to_info) as obj:
            obj.upload_fileobj(fobj, Config=self._transfer_config)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **_kwargs
    ):
        with self._get_obj(to_info) as obj:
            total = os.path.getsize(from_file)
            with Tqdm(
                disable=no_progress_bar, total=total, bytes=True, desc=name
            ) as pbar:
                obj.upload_file(
                    from_file,
                    Callback=pbar.update,
                    ExtraArgs=self.extra_args,
                    Config=self._transfer_config,
                )

    def _download(self, from_info, to_file, name=None, no_progress_bar=False):
        with self._get_obj(from_info) as obj:
            with Tqdm(
                disable=no_progress_bar,
                total=obj.content_length,
                bytes=True,
                desc=name,
            ) as pbar:
                obj.download_file(
                    to_file, Callback=pbar.update, Config=self._transfer_config
                )
