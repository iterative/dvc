import logging
import os
import threading

from funcy import cached_property, wrap_prop

from dvc.scheme import Schemes

from .fsspec_wrapper import ObjectFSWrapper

logger = logging.getLogger(__name__)


# pylint:disable=abstract-method
class OSSFileSystem(ObjectFSWrapper):
    scheme = Schemes.OSS
    REQUIRES = {"ossfs": "ossfs"}
    PARAM_CHECKSUM = "etag"
    COPY_POLL_SECONDS = 5
    LIST_OBJECT_PAGE_SIZE = 100

    def _prepare_credentials(self, **config):
        login_info = {}
        login_info["key"] = config.get("oss_key_id") or os.getenv(
            "OSS_ACCESS_KEY_ID"
        )
        login_info["secret"] = config.get("oss_key_secret") or os.getenv(
            "OSS_ACCESS_KEY_SECRET"
        )
        login_info["endpoint"] = config.get("oss_endpoint")
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from ossfs import OSSFileSystem as _OSSFileSystem

        return _OSSFileSystem(**self.fs_args)
