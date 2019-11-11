from __future__ import unicode_literals

from .config import RemoteConfig
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


def _get(config):
    for remote in REMOTES:
        if remote.supported(config):
            return remote
    return RemoteLOCAL


def Remote(repo, **kwargs):
    name = kwargs.get("name")
    if name:
        remote_config = RemoteConfig(repo.config)
        settings = remote_config.get_settings(name)
    else:
        settings = kwargs
    return _get(settings)(repo, settings)
