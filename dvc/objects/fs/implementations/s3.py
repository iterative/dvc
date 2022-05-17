import os
import threading
from collections import defaultdict

from funcy import cached_property, wrap_prop

from ..base import ObjectFileSystem
from ..errors import ConfigError
from ..utils import flatten, unflatten

_AWS_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".aws", "config")


# pylint:disable=abstract-method
class S3FileSystem(ObjectFileSystem):
    protocol = "s3"
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

        from ..utils import human_readable_to_bytes

        config, transfer_config = {}, {}
        for key, value in s3_config.items():
            if key in self._TRANSFER_CONFIG_ALIASES:
                if key in {"multipart_chunksize", "multipart_threshold"}:
                    # cast human readable sizes (like 24MiB) to integers
                    value = human_readable_to_bytes(value)
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
