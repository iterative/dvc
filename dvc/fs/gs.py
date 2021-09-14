import base64
import threading

from funcy import cached_property, wrap_prop

from dvc.path_info import CloudURLInfo
from dvc.scheme import Schemes

# pylint:disable=abstract-method
from .fsspec_wrapper import CallbackMixin, ObjectFSWrapper


class GSFileSystem(CallbackMixin, ObjectFSWrapper):
    scheme = Schemes.GS
    PATH_CLS = CloudURLInfo
    REQUIRES = {"gcsfs": "gcsfs"}
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))
    TRAVERSE_PREFIX_LEN = 2

    def _prepare_credentials(self, **config):
        login_info = {"consistency": None}
        login_info["project"] = config.get("projectname")
        login_info["token"] = config.get("credentialpath")
        return login_info

    def _entry_hook(self, entry):
        if "etag" in entry:
            entry["etag"] = base64.b64decode(entry["etag"]).hex()
        return entry

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from gcsfs import GCSFileSystem

        return GCSFileSystem(**self.fs_args)
