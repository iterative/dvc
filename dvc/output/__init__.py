import schema

from dvc.exceptions import DvcException

from dvc.dependency import SCHEMA as DEP_SCHEMA
from dvc.dependency.base import DependencyBase
from dvc.output.s3 import OutputS3
from dvc.output.gs import OutputGS
from dvc.output.local import OutputLOCAL


OUTS = [OutputS3, OutputGS, OutputLOCAL]

SCHEMA = DEP_SCHEMA
SCHEMA[schema.Optional(OutputLOCAL.PARAM_CACHE)] = bool


def _get(path):
    for o in OUTS:
        if o.supported(path):
            return o
    raise DvcException('Output \'{}\' is not supported'.format(path))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(DependencyBase.PARAM_PATH)
        cache = d.pop(OutputLOCAL.PARAM_CACHE, True)
        ret.append(_get(p)(stage, p, info=d, cache=cache))
    return ret


def loads_from(stage, s_list, use_cache=True):
    ret = []
    for s in s_list:
        ret.append(_get(s)(stage, s, info={}, cache=use_cache))
    return ret
