import os
import threading
from collections import defaultdict
from contextlib import contextmanager

from funcy import cached_property, wrap_prop

from dvc.exceptions import DvcException
from dvc.path_info import CloudURLInfo
from dvc.progress import Tqdm
from dvc.scheme import Schemes
from dvc.utils import error_link

from .fsspec_wrapper import FSSpecWrapper

_AWS_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".aws", "config")


class BaseS3FileSystem(FSSpecWrapper):
    scheme = Schemes.S3
    PATH_CLS = CloudURLInfo
    REQUIRES = {"s3fs": "s3fs"}
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))

    _GRANTS = {
        "grant_full_control": "GrantFullControl",
        "grant_read": "GrantRead",
        "grant_read_acp": "GrantReadACP",
        "grant_write_acp": "GrantWriteACP",
    }

    def __init__(self, repo, config):
        super().__init__(repo, config)

        url = config.get("url", "s3://")
        self.path_info = self.PATH_CLS(url)

        self._open_args = {}
        self.login_info = self._prepare_credentials(config)

    def _split_s3_config(self, s3_config):
        """Splits the general s3 config into 2 different config
        objects, one for transfer.TransferConfig and other is the
        general session config"""
        from dvc.utils import conversions

        config = {}
        for key, value in s3_config.items():
            if key in {"multipart_chunksize", "multipart_threshold"}:
                self._open_args[
                    "block_size"
                ] = conversions.human_readable_to_bytes(value)
            else:
                config[key] = value

        return config

    def _load_aws_config_file(self, profile):
        from botocore.configloader import load_config

        config_path = os.environ.get("AWS_CONFIG_FILE", _AWS_CONFIG_PATH)
        if not os.path.exists(config_path):
            return None

        config = load_config(config_path)
        profile_config = config["profiles"].get(profile or "default")
        if not profile_config:
            return None

        s3_config = profile_config.get("s3", {})
        return self._split_s3_config(s3_config)

    def _prepare_credentials(self, config):
        from dvc.config import ConfigError
        from dvc.utils.flatten import flatten, unflatten

        login_info = defaultdict(dict)

        # credentials
        login_info["key"] = config.get("access_key_id")
        login_info["secret"] = config.get("secret_access_key")
        login_info["token"] = config.get("session_token")

        # session configuration
        login_info["profile"] = config.get("profile")
        login_info["use_ssl"] = config.get("use_ssl", True)

        # extra client configuration
        client = login_info["client_kwargs"]
        client["region_name"] = config.get("region")
        client["endpoint_url"] = config.get("endpointurl")

        # encryptions
        additional = login_info["s3_additional_kwargs"]
        additional["ServerSideEncryption"] = config.get("sse")
        additional["SSEKMSKeyId"] = config.get("sse_kms_key_id")
        additional["ACL"] = config.get("acl")
        for grant_option, grant_key in self._GRANTS.items():
            if config.get(grant_option):
                if additional["ACL"]:
                    raise ConfigError(
                        "`acl` and `grant_*` AWS S3 config options "
                        "are mutually exclusive"
                    )
                additional[grant_key] = config[grant_option]

        # config kwargs
        session_config = login_info["config_kwargs"]
        session_config["s3"] = self._load_aws_config_file(
            login_info["profile"]
        )

        return unflatten(
            {
                key: value
                for key, value in flatten(login_info).items()
                if value is not None
            }
        )

    def _entry_hook(self, entry):
        entry = entry.copy()
        if "ETag" in entry:
            entry["etag"] = entry["ETag"].strip('"')
        return entry

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from s3fs import S3FileSystem as _S3FileSystem

        return _S3FileSystem(**self.login_info, skip_instance_cache=True)


class S3FileSystem(BaseS3FileSystem):
    @wrap_prop(threading.Lock())
    @cached_property
    def s3(self):
        import boto3

        login_info = self.login_info
        client_kwargs = login_info.get("client_kwargs", {})
        session_opts = {
            "profile_name": login_info.get("profile"),
            "region_name": client_kwargs.get("region_name"),
        }

        if "key" in login_info:
            session_opts["aws_access_key_id"] = login_info["key"]
        if "secret" in login_info:
            session_opts["aws_secret_access_key"] = login_info["secret"]
        if "token" in login_info:
            session_opts["aws_session_token"] = login_info["token"]

        session = boto3.session.Session(**session_opts)
        # pylint: disable=attribute-defined-outside-init
        self.endpoint_url = client_kwargs.get("endpoint_url")
        return session.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            use_ssl=login_info["use_ssl"],
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
                    ExtraArgs=self.login_info.get("s3_additional_kwargs"),
                )
        self.fs.invalidate_cache(self._with_bucket(to_info.parent))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **pbar_args
    ):
        with self._get_obj(from_info) as obj:
            with Tqdm(
                disable=no_progress_bar,
                total=obj.content_length,
                bytes=True,
                desc=name,
                **pbar_args,
            ) as pbar:
                obj.download_file(to_file, Callback=pbar.update)
