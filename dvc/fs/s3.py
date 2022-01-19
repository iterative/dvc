import functools
import os
import threading
from collections import defaultdict

from funcy import cached_property, wrap_prop

from dvc.scheme import Schemes

from ._callback import DEFAULT_CALLBACK
from .fsspec_wrapper import ObjectFSWrapper

_AWS_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".aws", "config")


# pylint:disable=abstract-method
class BaseS3FileSystem(ObjectFSWrapper):
    scheme = Schemes.S3
    REQUIRES = {"s3fs": "s3fs", "boto3": "boto3"}
    PARAM_CHECKSUM = "etag"

    _GRANTS = {
        "grant_full_control": "GrantFullControl",
        "grant_read": "GrantRead",
        "grant_read_acp": "GrantReadACP",
        "grant_write_acp": "GrantWriteACP",
    }

    _TRANSFER_CONFIG_ALIASES = {
        "max_queue_size": "max_io_queue",
        "max_concurrent_requests": "max_concurrency",
        "multipart_threshold": "multipart_threshold",
        "multipart_chunksize": "multipart_chunksize",
    }

    def _split_s3_config(self, s3_config):
        """Splits the general s3 config into 2 different config
        objects, one for transfer.TransferConfig and other is the
        general session config"""

        from boto3.s3.transfer import TransferConfig

        from dvc.utils import conversions

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

        # pylint: disable=attribute-defined-outside-init
        self._transfer_config = TransferConfig(**transfer_config)
        return config

    def _load_aws_config_file(self, profile):
        from botocore.configloader import load_config

        # pylint: disable=attribute-defined-outside-init
        self._transfer_config = None
        config_path = os.environ.get("AWS_CONFIG_FILE", _AWS_CONFIG_PATH)
        if not os.path.exists(config_path):
            return {}

        config = load_config(config_path)
        profile_config = config["profiles"].get(profile or "default")
        if not profile_config:
            return {}

        s3_config = profile_config.get("s3", {})
        return self._split_s3_config(s3_config)

    def _prepare_credentials(self, **config):
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
        client["verify"] = config.get("ssl_verify")

        # timeout configuration
        config_kwargs = login_info["config_kwargs"]
        config_kwargs["read_timeout"] = config.get("read_timeout")
        config_kwargs["connect_timeout"] = config.get("connect_timeout")

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

        shared_creds = config.get("credentialpath")
        if shared_creds:
            os.environ.setdefault("AWS_SHARED_CREDENTIALS_FILE", shared_creds)

        if (
            client["region_name"] is None
            and session_config["s3"].get("region_name") is None
            and os.getenv("AWS_REGION") is None
        ):
            # Enable bucket region caching
            login_info["cache_regions"] = config.get("cache_regions", True)

        config_path = config.get("configpath")
        if config_path:
            os.environ.setdefault("AWS_CONFIG_FILE", config_path)

        return unflatten(
            {
                key: value
                for key, value in flatten(login_info).items()
                if value is not None
            }
        )

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from s3fs import S3FileSystem as _S3FileSystem

        return _S3FileSystem(**self.fs_args)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path):
        return "s3://" + path.lstrip("/")


def _translate_exceptions(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as exc:
            from s3fs.errors import translate_boto_error

            raise translate_boto_error(exc)

    return wrapper


class S3FileSystem(BaseS3FileSystem):  # pylint:disable=abstract-method
    @wrap_prop(threading.Lock())
    @cached_property
    def s3(self):
        import boto3

        login_info = self.fs_args
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

        return session.resource(
            "s3",
            endpoint_url=client_kwargs.get("endpoint_url"),
            use_ssl=login_info["use_ssl"],
            verify=client_kwargs.get("verify"),
        )

    def _get_obj(self, path):
        bucket_name, key, _ = self.fs.split_path(path)
        bucket = self.s3.Bucket(bucket_name)
        return bucket.Object(key)

    @_translate_exceptions
    def put_file(
        self, from_file, to_info, callback=DEFAULT_CALLBACK, **kwargs
    ):
        callback.set_size(os.path.getsize(from_file))
        obj = self._get_obj(to_info)
        obj.upload_file(
            from_file,
            Callback=callback.relative_update,
            ExtraArgs=self.fs_args.get("s3_additional_kwargs"),
            Config=self._transfer_config,
        )
        self.fs.invalidate_cache(self.path.parent(to_info))

    @_translate_exceptions
    def get_file(
        self, from_info, to_info, callback=DEFAULT_CALLBACK, **kwargs
    ):
        obj = self._get_obj(from_info)
        callback.set_size(obj.content_length)
        obj.download_file(to_info, Callback=callback.relative_update)
