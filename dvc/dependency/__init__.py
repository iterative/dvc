import schema

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from dvc.exceptions import DvcException
from dvc.config import Config

from dvc.dependency.base import DependencyBase
from dvc.dependency.s3 import DependencyS3
from dvc.dependency.gs import DependencyGS
from dvc.dependency.local import DependencyLOCAL
from dvc.dependency.hdfs import DependencyHDFS
from dvc.dependency.ssh import DependencySSH
from dvc.dependency.http import DependencyHTTP

from dvc.remote import Remote
from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3
from dvc.remote.hdfs import RemoteHDFS

DEPS = [
    DependencyGS,
    DependencyHDFS,
    DependencyHTTP,
    DependencyLOCAL,
    DependencyS3,
    DependencySSH,
]

DEP_MAP = {
    '': DependencyLOCAL,
    'ssh': DependencySSH,
    's3': DependencyS3,
    'gs': DependencyGS,
    'hdfs': DependencyHDFS,
    'http': DependencyHTTP,
    'https': DependencyHTTP,
}

# We are skipping RemoteHTTP.PARAM_ETAG because is the same as RemoteS3
SCHEMA = {
    DependencyBase.PARAM_PATH: str,
    schema.Optional(RemoteLOCAL.PARAM_MD5): schema.Or(str, None),
    schema.Optional(RemoteS3.PARAM_ETAG): schema.Or(str, None),
    schema.Optional(RemoteHDFS.PARAM_CHECKSUM): schema.Or(str, None),
}


def _get(stage, p, info):
    parsed = urlparse(p)
    if parsed.scheme == 'remote':
        name = Config.SECTION_REMOTE_FMT.format(parsed.netloc)
        sect = stage.project.config._config[name]
        remote = Remote(stage.project, sect)
        return DEP_MAP[remote.scheme](stage, p, info, remote=remote)

    for d in DEPS:
        if d.supported(p):
            return d(stage, p, info)
    raise DvcException('Dependency \'{}\' is not supported'.format(p))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(DependencyBase.PARAM_PATH)
        ret.append(_get(stage, p, d))
    return ret


def loads_from(stage, s_list):
    ret = []
    for s in s_list:
        ret.append(_get(stage, s, {}))
    return ret
