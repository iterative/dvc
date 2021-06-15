import threading

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes

from .fsspec_wrapper import ObjectFSWrapper


# pylint:disable=abstract-method
class OSSFileSystem(ObjectFSWrapper):
    scheme = Schemes.OSS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"ossfs": "ossfs"}
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))

    def _prepare_credentials(self, **config):
        login_info = {}
        login_info["key"] = config.get("oss_key_id")
        login_info["secret"] = config.get("oss_key_secret")
        login_info["endpoint"] = config.get("oss_endpoint")
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from ossfs import OSSFileSystem as _OSSFileSystem

        return _OSSFileSystem(**self.fs_args)
