from urllib.parse import urlparse

from . import generic  # noqa: F401
from .implementations.azure import AzureFileSystem
from .implementations.gdrive import GDriveFileSystem
from .implementations.gs import GSFileSystem
from .implementations.hdfs import HDFSFileSystem
from .implementations.http import HTTPFileSystem
from .implementations.https import HTTPSFileSystem
from .implementations.local import LocalFileSystem
from .implementations.memory import MemoryFileSystem  # noqa: F401
from .implementations.oss import OSSFileSystem
from .implementations.s3 import S3FileSystem
from .implementations.ssh import SSHFileSystem
from .implementations.webdav import WebDAVFileSystem, WebDAVSFileSystem
from .implementations.webhdfs import WebHDFSFileSystem
from .scheme import Schemes

FS_MAP = {
    Schemes.AZURE: AzureFileSystem,
    Schemes.GDRIVE: GDriveFileSystem,
    Schemes.GS: GSFileSystem,
    Schemes.HDFS: HDFSFileSystem,
    Schemes.WEBHDFS: WebHDFSFileSystem,
    Schemes.HTTP: HTTPFileSystem,
    Schemes.HTTPS: HTTPSFileSystem,
    Schemes.S3: S3FileSystem,
    Schemes.SSH: SSHFileSystem,
    Schemes.OSS: OSSFileSystem,
    Schemes.WEBDAV: WebDAVFileSystem,
    Schemes.WEBDAVS: WebDAVSFileSystem,
    # NOTE: LocalFileSystem is the default
}


def _import_class(cls: str):
    """Take a string FQP and return the imported class or identifier

    clas is of the form "package.module.klass".
    """
    import importlib

    mod, name = cls.rsplit(".", maxsplit=1)
    module = importlib.import_module(mod)
    return getattr(module, name)


def get_fs_cls(remote_conf, cls=None, scheme=None):
    if cls:
        return _import_class(cls)

    if not scheme:
        scheme = urlparse(remote_conf["url"]).scheme
    return FS_MAP.get(scheme, LocalFileSystem)
