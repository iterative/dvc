import posixpath
from urllib.parse import urlparse

from .azure import AzureTree
from .gdrive import GDriveTree
from .gs import GSTree
from .hdfs import HDFSTree
from .http import HTTPTree
from .https import HTTPSTree
from .local import LocalTree
from .oss import OSSTree
from .s3 import S3Tree
from .ssh import SSHTree
from .webdav import WebDAVTree
from .webdavs import WebDAVSTree
from .webhdfs import WebHDFSTree

TREES = [
    AzureTree,
    GDriveTree,
    GSTree,
    HDFSTree,
    HTTPTree,
    HTTPSTree,
    S3Tree,
    SSHTree,
    OSSTree,
    WebDAVTree,
    WebDAVSTree,
    WebHDFSTree
    # NOTE: LocalTree is the default
]


def get_tree_cls(remote_conf):
    for tree_cls in TREES:
        if tree_cls.supported(remote_conf):
            return tree_cls
    return LocalTree


def get_tree_config(config, **kwargs):
    name = kwargs.get("name")
    if name:
        remote_conf = config["remote"][name.lower()]
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

    base = get_tree_config(config, name=parsed.netloc)
    url = posixpath.join(base["url"], parsed.path.lstrip("/"))
    return {**base, **remote_conf, "url": url}


def get_cloud_tree(repo, **kwargs):
    from dvc.config import SCHEMA, ConfigError, Invalid

    remote_conf = get_tree_config(repo.config, **kwargs)
    try:
        remote_conf = SCHEMA["remote"][str](remote_conf)
    except Invalid as exc:
        raise ConfigError(str(exc)) from None
    return get_tree_cls(remote_conf)(repo, remote_conf)
