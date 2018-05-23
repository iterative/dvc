import schema

from dvc.exceptions import DvcException

from dvc.dependency.base import DependencyBase
from dvc.dependency.s3 import DependencyS3
from dvc.dependency.gs import DependencyGS
from dvc.dependency.local import DependencyLOCAL


DEPS = [DependencyS3, DependencyGS, DependencyLOCAL]

SCHEMA = {
    DependencyBase.PARAM_PATH: str,
    schema.Optional(DependencyLOCAL.PARAM_MD5): schema.Or(str, None),
    schema.Optional(DependencyS3.PARAM_ETAG): schema.Or(str, None),
}


def _get(path):
    for d in DEPS:
        if d.supported(path):
            return d
    raise DvcException('Dependency \'{}\' is not supported'.format(path))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d[DependencyBase.PARAM_PATH]
        ret.append(_get(p).loadd(stage, d))
    return ret


def loads_from(stage, s_list):
    ret = []
    for s in s_list:
        ret.append(_get(s).loads(stage, s))
    return ret
