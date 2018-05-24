import schema

from dvc.exceptions import DvcException

import dvc.dependency as dependency
from dvc.dependency.base import DependencyBase
from dvc.output.s3 import OutputS3
from dvc.output.gs import OutputGS
from dvc.output.local import OutputLOCAL


OUTS = [OutputS3, OutputGS, OutputLOCAL]

SCHEMA = dependency.SCHEMA
SCHEMA[schema.Optional(OutputLOCAL.PARAM_CACHE)] = bool


def _get(path):
    for o in OUTS:
        if o.supported(path):
            return o
    raise DvcException('Output \'{}\' is not supported'.format(path))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d[DependencyBase.PARAM_PATH]
        ret.append(_get(p).loadd(stage, d))
    return ret


def loads_from(stage, s_list, use_cache=False):
    ret = []
    for s in s_list:
        ret.append(_get(s).loads(stage, s, use_cache=use_cache))
    return ret
