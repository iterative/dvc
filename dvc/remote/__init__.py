from __future__ import unicode_literals

from dvc.remote.azure import RemoteAzure
from dvc.remote.gs import RemoteGS
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3
from dvc.remote.ssh import RemoteSSH
from dvc.remote.http import RemoteHTTP
from dvc.remote.oss import RemoteOSS


REMOTES = [
    RemoteAzure,
    RemoteGS,
    RemoteHDFS,
    RemoteHTTP,
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


def Remote(repo, config):
    return _get(config)(repo, config)
