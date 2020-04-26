import posixpath
from urllib.parse import urlparse

from dvc.remote.azure import AzureRemote
from dvc.remote.gdrive import GDriveRemote
from dvc.remote.gs import GSRemote
from dvc.remote.hdfs import HDFSRemote
from dvc.remote.http import HTTPRemote
from dvc.remote.https import HTTPSRemote
from dvc.remote.local import LocalRemote
from dvc.remote.oss import OSSRemote
from dvc.remote.s3 import S3Remote
from dvc.remote.ssh import SSHRemote


REMOTES = [
    AzureRemote,
    GDriveRemote,
    GSRemote,
    HDFSRemote,
    HTTPRemote,
    HTTPSRemote,
    S3Remote,
    SSHRemote,
    OSSRemote,
    # NOTE: LocalRemote is the default
]


def _get(remote_conf):
    for remote in REMOTES:
        if remote.supported(remote_conf):
            return remote
    return LocalRemote


def Remote(repo, **kwargs):
    name = kwargs.get("name")
    if name:
        remote_conf = repo.config["remote"][name.lower()]
    else:
        remote_conf = kwargs
    remote_conf = _resolve_remote_refs(repo.config, remote_conf)
    return _get(remote_conf)(repo, remote_conf)


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

    base = config["remote"][parsed.netloc]
    url = posixpath.join(base["url"], parsed.path.lstrip("/"))
    return {**base, **remote_conf, "url": url}
