import threading

from funcy import cached_property, wrap_prop

# pylint:disable=abstract-method
from ..base import ObjectFileSystem


class GSFileSystem(ObjectFileSystem):
    protocol = "gs"
    REQUIRES = {"gcsfs": "gcsfs"}
    PARAM_CHECKSUM = "etag"

    def _prepare_credentials(self, **config):
        login_info = {"consistency": None}
        login_info["project"] = config.get("projectname")
        login_info["token"] = config.get("credentialpath")
        return login_info

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from gcsfs import GCSFileSystem

        return GCSFileSystem(**self.fs_args)
