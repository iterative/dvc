from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3
from dvc.remote.gs import RemoteGS

from dvc.config import Config
from dvc.exceptions import DvcException


def Remote(project, config):
    for r in [RemoteLOCAL, RemoteS3, RemoteGS]:
        if r.supported(config):
            return r(project, config)
    raise DvcException('Remote \'{}\' is not supported.'.format(config))
