from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3
from dvc.remote.gs import RemoteGS
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.ssh import RemoteSSH
from dvc.remote.azure import RemoteAzure

from dvc.config import Config
from dvc.exceptions import UnsupportedRemoteError


REMOTES = [RemoteHDFS, RemoteSSH, RemoteS3, RemoteGS, RemoteAzure, RemoteLOCAL]


def supported_url(url):
    config = {Config.SECTION_REMOTE_URL: url}
    return any(remote.supported(config) for remote in REMOTES)


def Remote(project, config):
    for remote in REMOTES:
        if remote.supported(config):
            return remote(project, config)
    raise UnsupportedRemoteError(str(config))
