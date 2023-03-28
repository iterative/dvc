from urllib.parse import urlparse

from dvc_http import HTTPFileSystem, HTTPSFileSystem  # noqa: F401

from dvc.config import ConfigError as RepoConfigError
from dvc.config_schema import SCHEMA, Invalid

# pylint: disable=unused-import
from dvc_objects.fs import (  # noqa: F401
    LocalFileSystem,
    MemoryFileSystem,
    Schemes,
    generic,
    get_fs_cls,
    known_implementations,
    localfs,
    registry,
    system,
    utils,
)
from dvc_objects.fs.base import AnyFSPath, FileSystem  # noqa: F401
from dvc_objects.fs.errors import (  # noqa: F401
    AuthError,
    ConfigError,
    RemoteMissingDepsError,
)
from dvc_objects.fs.path import Path  # noqa: F401

from .callbacks import Callback
from .data import DataFileSystem  # noqa: F401
from .dvc import DVCFileSystem  # noqa: F401
from .git import GitFileSystem  # noqa: F401

known_implementations.update(
    {
        "dvc": {
            "class": "dvc.fs.dvc.DVCFileSystem",
            "err": "dvc is supported, but requires 'dvc' to be installed",
        },
        "git": {
            "class": "dvc.fs.git.GitFileSystem",
            "err": "git is supported, but requires 'dvc' to be installed",
        },
    }
)


# pylint: enable=unused-import


def download(fs, fs_path, to, jobs=None):
    with Callback.as_tqdm_callback(
        desc=f"Downloading {fs.path.name(fs_path)}",
        unit="files",
    ) as cb:
        fs.get(fs_path, to.fs_path, batch_size=jobs, callback=cb)


def parse_external_url(url, config=None):
    remote_config = dict(config) if config else {}
    remote_config["url"] = url
    fs_cls, fs_config, fs_path = get_cloud_fs(None, **remote_config)
    fs = fs_cls(**fs_config)
    return fs, fs_path


def get_fs_config(config, **kwargs):
    name = kwargs.get("name")
    if name:
        try:
            remote_conf = config["remote"][name.lower()]
        except KeyError:
            from dvc.config import RemoteNotFoundError

            raise RemoteNotFoundError(f"remote '{name}' doesn't exist")  # noqa: B904
    else:
        remote_conf = kwargs
    return _resolve_remote_refs(config, remote_conf)


def _resolve_remote_refs(config, remote_conf):
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

    base = get_fs_config(config, name=parsed.netloc)
    cls, _, _ = _get_cloud_fs(config, **base)
    relpath = parsed.path.lstrip("/").replace("/", cls.sep)
    url = cls.sep.join((base["url"], relpath))
    return {**base, **remote_conf, "url": url}


def get_cloud_fs(repo, **kwargs):
    repo_config = repo.config if repo else {}
    return _get_cloud_fs(repo_config, **kwargs)


def _get_cloud_fs(repo_config, **kwargs):
    core_config = repo_config.get("core", {})

    remote_conf = get_fs_config(repo_config, **kwargs)
    try:
        remote_conf = SCHEMA["remote"][str](remote_conf)  # type: ignore[index]
    except Invalid as exc:
        raise RepoConfigError(str(exc)) from None

    if "checksum_jobs" not in remote_conf:
        checksum_jobs = core_config.get("checksum_jobs")
        if checksum_jobs:
            remote_conf["checksum_jobs"] = checksum_jobs

    cls = get_fs_cls(remote_conf)

    url = remote_conf.pop("url")
    if cls.protocol in ["webdav", "webdavs"]:
        # For WebDAVFileSystem, provided url is the base path itself, so it
        # should be treated as being a root path.
        fs_path = cls.root_marker
    else:
        fs_path = cls._strip_protocol(url)  # pylint:disable=protected-access

    extras = cls._get_kwargs_from_urls(url)  # pylint:disable=protected-access
    conf = {**extras, **remote_conf}  # remote config takes priority
    return cls, conf, fs_path
