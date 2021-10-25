import threading

from funcy import cached_property, wrap_prop

from dvc.scheme import Schemes

# pylint:disable=abstract-method
from .fsspec_wrapper import CallbackMixin, FSSpecWrapper


class WebHDFSFileSystem(CallbackMixin, FSSpecWrapper):
    scheme = Schemes.WEBHDFS
    REQUIRES = {"fsspec": "fsspec"}
    PARAM_CHECKSUM = "checksum"

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        from fsspec.utils import infer_storage_options

        return infer_storage_options(path)["path"]

    def unstrip_protocol(self, path: str) -> str:
        host = self.fs_args["host"]
        port = self.fs_args["port"]
        path = path.lstrip("/")
        return f"webhdfs://{host}:{port}/{path}"

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.implementations.webhdfs import WebHDFS

        return (
            WebHDFS._get_kwargs_from_urls(  # pylint:disable=protected-access
                urlpath
            )
        )

    def _prepare_credentials(self, **config):
        self._ssl_verify = config.pop("ssl_verify", True)
        principal = config.pop("kerberos_principal", None)
        if principal:
            config["kerb_kwargs"] = {"principal": principal}
        return config

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from fsspec.implementations.webhdfs import WebHDFS

        fs = WebHDFS(**self.fs_args)
        fs.session.verify = self._ssl_verify
        return fs

    def checksum(self, path):
        ukey = self.fs.ukey(path)
        return ukey["bytes"]
