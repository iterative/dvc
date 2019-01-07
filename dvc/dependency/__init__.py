import schema

from dvc.config import Config
from dvc.utils.compat import urlparse

import dvc.output as output
from dvc.output.base import OutputBase
from dvc.dependency.s3 import DependencyS3
from dvc.dependency.gs import DependencyGS
from dvc.dependency.local import DependencyLOCAL
from dvc.dependency.hdfs import DependencyHDFS
from dvc.dependency.ssh import DependencySSH
from dvc.dependency.http import DependencyHTTP

from dvc.remote import Remote

DEPS = [
    DependencyGS,
    DependencyHDFS,
    DependencyHTTP,
    DependencyS3,
    DependencySSH,
    # NOTE: DependencyLOCAL is the default choice
]

DEP_MAP = {
    'local': DependencyLOCAL,
    'ssh': DependencySSH,
    's3': DependencyS3,
    'gs': DependencyGS,
    'hdfs': DependencyHDFS,
    'http': DependencyHTTP,
    'https': DependencyHTTP,
}

SCHEMA = output.SCHEMA.copy()
del SCHEMA[schema.Optional(OutputBase.PARAM_CACHE)]
del SCHEMA[schema.Optional(OutputBase.PARAM_METRIC)]


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
    return DependencyLOCAL(stage, p, info)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(OutputBase.PARAM_PATH)
        ret.append(_get(stage, p, d))
    return ret


def loads_from(stage, s_list):
    ret = []
    for s in s_list:
        ret.append(_get(stage, s, {}))
    return ret
