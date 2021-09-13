import logging
import threading
from functools import lru_cache

from funcy import cached_property, wrap_prop

from dvc.path_info import WebDAVURLInfo
from dvc.scheme import Schemes

from .fsspec_wrapper import FSSpecWrapper
from .http import ask_password

logger = logging.getLogger(__name__)


class WebDAVFileSystem(FSSpecWrapper):  # pylint:disable=abstract-method
    scheme = Schemes.WEBDAV
    PATH_CLS = WebDAVURLInfo
    CAN_TRAVERSE = True
    TRAVERSE_PREFIX_LEN = 2
    REQUIRES = {"webdav4": "webdav4"}
    PARAM_CHECKSUM = "etag"
    DETAIL_FIELDS = frozenset(("etag", "size"))

    def __init__(self, **config):
        super().__init__(**config)

        cert_path = config.get("cert_path", None)
        key_path = config.get("key_path", None)
        self.prefix = config.get("prefix", "")
        self.fs_args.update(
            {
                "base_url": config["url"],
                "cert": cert_path if not key_path else (cert_path, key_path),
                "verify": config.get("ssl_verify", True),
                "timeout": config.get("timeout", 30),
            }
        )

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        path_info = WebDAVURLInfo(urlpath)
        return {
            "prefix": path_info.path.rstrip("/"),
            "host": path_info.replace(path="").url,
            "url": path_info.url.rstrip("/"),
            "user": path_info.user,
        }

    def _prepare_credentials(self, **config):
        user = config.get("user", None)
        password = config.get("password", None)

        headers = {}
        token = config.get("token")
        auth = None
        if token:
            headers.update({"Authorization": f"Bearer {token}"})
        elif user:
            if not password and config.get("ask_password"):
                password = ask_password(config["host"], user)
            auth = (user, password)

        return {"headers": headers, "auth": auth}

    @wrap_prop(threading.Lock())
    @cached_property
    def fs(self):
        from webdav4.fsspec import WebdavFileSystem

        return WebdavFileSystem(**self.fs_args)

    def upload_fobj(self, fobj, to_info, **kwargs):
        rpath = self.translate_path_info(to_info)
        size = kwargs.get("size")
        # using upload_fileobj to directly upload fileobj rather than buffering
        # and using overwrite=True to avoid check for an extra exists call,
        # as caller should ensure that the file does not exist beforehand.
        return self.fs.upload_fileobj(fobj, rpath, overwrite=True, size=size)

    @lru_cache(512)
    def translate_path_info(self, path):
        if isinstance(path, self.PATH_CLS):
            return path.path[len(self.prefix) :].lstrip("/")
        return path

    _with_bucket = translate_path_info


class WebDAVSFileSystem(WebDAVFileSystem):  # pylint:disable=abstract-method
    scheme = Schemes.WEBDAVS
