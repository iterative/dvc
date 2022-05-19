from urllib.parse import urlparse

# pylint: disable=unused-import
from dvc_objects.fs import utils  # noqa: F401
from dvc_objects.fs import (  # noqa: F401
    FS_MAP,
    AzureFileSystem,
    GDriveFileSystem,
    GSFileSystem,
    HDFSFileSystem,
    HTTPFileSystem,
    HTTPSFileSystem,
    LocalFileSystem,
    MemoryFileSystem,
    OSSFileSystem,
    S3FileSystem,
    Schemes,
    SSHFileSystem,
    WebDAVFileSystem,
    WebDAVSFileSystem,
    WebHDFSFileSystem,
    generic,
    get_fs_cls,
    system,
)
from dvc_objects.fs.base import AnyFSPath, FileSystem  # noqa: F401
from dvc_objects.fs.errors import (  # noqa: F401
    AuthError,
    ConfigError,
    RemoteMissingDepsError,
)
from dvc_objects.fs.implementations.azure import AzureAuthError  # noqa: F401
from dvc_objects.fs.implementations.gdrive import GDriveAuthError  # noqa: F401
from dvc_objects.fs.implementations.local import localfs  # noqa: F401
from dvc_objects.fs.implementations.ssh import (  # noqa: F401
    DEFAULT_PORT as DEFAULT_SSH_PORT,
)
from dvc_objects.fs.path import Path  # noqa: F401

from .data import DataFileSystem  # noqa: F401
from .dvc import DvcFileSystem  # noqa: F401
from .git import GitFileSystem  # noqa: F401

# pylint: enable=unused-import


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
    from dvc.config import ConfigError as RepoConfigError
    from dvc.config_schema import SCHEMA, Invalid

    repo_config = repo.config if repo else {}
    core_config = repo_config.get("core", {})

    remote_conf = get_fs_config(repo, repo_config, **kwargs)
    try:
        remote_conf = SCHEMA["remote"][str](remote_conf)
    except Invalid as exc:
        raise RepoConfigError(str(exc)) from None

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
