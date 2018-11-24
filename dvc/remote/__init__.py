from dvc.remote.azure import RemoteAzure
from dvc.remote.gs import RemoteGS
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3
from dvc.remote.ssh import RemoteSSH
from dvc.remote.http import RemoteHTTP

from dvc.config import Config
from dvc.exceptions import UnsupportedRemoteError


REMOTES = [
    RemoteAzure,
    RemoteGS,
    RemoteHDFS,
    RemoteHTTP,
    RemoteLOCAL,
    RemoteS3,
    RemoteSSH,
]


def supported_url(url):
    config = {Config.SECTION_REMOTE_URL: url}
    return any(remote.supported(config) for remote in REMOTES)


def Remote(project, config):
    for remote in REMOTES:
        if remote.supported(config):
            return remote(project, config)
    raise UnsupportedRemoteError(str(config))
