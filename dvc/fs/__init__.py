from urllib.parse import urlparse

from ..scheme import Schemes
from . import utils  # noqa: F401
from .azure import AzureFileSystem
from .gdrive import GDriveFileSystem
from .gs import GSFileSystem
from .hdfs import HDFSFileSystem
from .http import HTTPFileSystem
from .https import HTTPSFileSystem
from .local import LocalFileSystem
from .oss import OSSFileSystem
from .s3 import S3FileSystem
from .ssh import SSHFileSystem
from .webdav import WebDAVFileSystem, WebDAVSFileSystem
from .webhdfs import WebHDFSFileSystem

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


def get_fs_cls(remote_conf, scheme=None):
    if not scheme:
        scheme = urlparse(remote_conf["url"]).scheme
    return FS_MAP.get(scheme, LocalFileSystem)


def get_fs_config(repo, config, **kwargs):
    name = kwargs.get("name")
    if name:
        try:
            remote_conf = config["remote"][name.lower()]
        except KeyError:
            from dvc.config import RemoteNotFoundError

            raise RemoteNotFoundError(f"remote '{name}' doesn't exist")
    else:
        remote_conf = kwargs
    return _resolve_remote_refs(repo, config, remote_conf)


def _resolve_remote_refs(repo, config, remote_conf):
    # Support for cross referenced remotes.
    # This will merge the settings, shadowing base ref with remote_conf.
    # For example, having:
    #
    #       dvc remote add server ssh://localhost
    #       dvc remote modify server user root
    #       dvc remote modify server ask_password true
    #
    #       dvc remote add images remote://server/tmp/pictures
    #       dvc remote modify images user alice
    #       dvc remote modify images ask_password false
    #       dvc remote modify images password asdf1234
    #
    # Results on a config dictionary like:
    #
    #       {
    #           "url": "ssh://localhost/tmp/pictures",
    #           "user": "alice",
    #           "password": "asdf1234",
    #           "ask_password": False,
    #       }
    parsed = urlparse(remote_conf["url"])
    if parsed.scheme != "remote":
        return remote_conf

    base = get_fs_config(repo, config, name=parsed.netloc)
    cls, _, _ = get_cloud_fs(repo, **base)
    relpath = parsed.path.lstrip("/").replace("/", cls.sep)
    url = cls.sep.join((base["url"], relpath))
    return {**base, **remote_conf, "url": url}


def get_cloud_fs(repo, **kwargs):
    from dvc.config import ConfigError
    from dvc.config_schema import SCHEMA, Invalid

    repo_config = repo.config if repo else {}
    core_config = repo_config.get("core", {})

    remote_conf = get_fs_config(repo, repo_config, **kwargs)
    try:
        remote_conf = SCHEMA["remote"][str](remote_conf)
    except Invalid as exc:
        raise ConfigError(str(exc)) from None

    if "jobs" not in remote_conf:
        jobs = core_config.get("jobs")
        if jobs:
            remote_conf["jobs"] = jobs

    if "checksum_jobs" not in remote_conf:
        checksum_jobs = core_config.get("checksum_jobs")
        if checksum_jobs:
            remote_conf["checksum_jobs"] = checksum_jobs

    cls = get_fs_cls(remote_conf)

    if cls == GDriveFileSystem and repo:
        remote_conf["gdrive_credentials_tmp_dir"] = repo.tmp_dir

    url = remote_conf.pop("url")
    if issubclass(cls, WebDAVFileSystem):
        # For WebDAVFileSystem, provided url is the base path itself, so it
        # should be treated as being a root path.
        fs_path = cls.root_marker
    else:
        fs_path = cls._strip_protocol(url)  # pylint:disable=protected-access

    extras = cls._get_kwargs_from_urls(url)  # pylint:disable=protected-access
    conf = {**extras, **remote_conf}  # remote config takes priority
    return cls, conf, fs_path
