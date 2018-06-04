import schema

from dvc.exceptions import DvcException

from dvc.dependency.base import DependencyBase
from dvc.dependency.s3 import DependencyS3
from dvc.dependency.gs import DependencyGS
from dvc.dependency.local import DependencyLOCAL

from dvc.remote.local import RemoteLOCAL
from dvc.remote.s3 import RemoteS3


DEPS = [DependencyS3, DependencyGS, DependencyLOCAL]

SCHEMA = {
    DependencyBase.PARAM_PATH: str,
    schema.Optional(RemoteLOCAL.PARAM_MD5): schema.Or(str, None),
    schema.Optional(RemoteS3.PARAM_ETAG): schema.Or(str, None),
}


def _get(path):
    for d in DEPS:
        if d.supported(path):
            return d
    raise DvcException('Dependency \'{}\' is not supported'.format(path))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(DependencyBase.PARAM_PATH)
        ret.append(_get(p)(stage, p, d))
    return ret


def loads_from(stage, s_list):
    ret = []
    for s in s_list:
        ret.append(_get(s)(stage, s, {}))
    return ret
