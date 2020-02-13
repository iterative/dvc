import posixpath
from urllib.parse import urlparse

from dvc.remote.azure import RemoteAZURE
from dvc.remote.gdrive import RemoteGDrive
from dvc.remote.gs import RemoteGS
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.http import RemoteHTTP
from dvc.remote.https import RemoteHTTPS
from dvc.remote.local import RemoteLOCAL
from dvc.remote.oss import RemoteOSS
from dvc.remote.s3 import RemoteS3
from dvc.remote.ssh import RemoteSSH


REMOTES = [
    RemoteAZURE,
    RemoteGDrive,
    RemoteGS,
    RemoteHDFS,
    RemoteHTTP,
    RemoteHTTPS,
    RemoteS3,
    RemoteSSH,
    RemoteOSS,
    # NOTE: RemoteLOCAL is the default
]


def _get(remote_conf):
    for remote in REMOTES:
        if remote.supported(remote_conf):
            return remote
    return RemoteLOCAL


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
