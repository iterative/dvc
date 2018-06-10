import schema

from dvc.exceptions import DvcException
from dvc.config import Config

from dvc.dependency import SCHEMA, urlparse
from dvc.dependency.base import DependencyBase
from dvc.output.s3 import OutputS3
from dvc.output.gs import OutputGS
from dvc.output.local import OutputLOCAL
from dvc.output.hdfs import OutputHDFS

from dvc.remote import Remote


OUTS = [OutputHDFS, OutputS3, OutputGS, OutputLOCAL]

OUTS_MAP = {'hdfs': OutputHDFS,
            's3': OutputS3,
            'gs': OutputGS,
            '': OutputLOCAL}

SCHEMA[schema.Optional(OutputLOCAL.PARAM_CACHE)] = bool


def _get(stage, p, info, cache):
    parsed = urlparse(p)
    if parsed.scheme == 'remote':
        sect = stage.project.config._config[Config.SECTION_REMOTE_FMT.format(parsed.netloc)]
        remote = Remote(stage.project, sect)
        return OUTS_MAP[remote.scheme](stage, p, info, cache=cache, remote=remote)

    for o in OUTS:
        if o.supported(p):
            return o(stage, p, info, cache=cache, remote=None)
    raise DvcException('Output \'{}\' is not supported'.format(p))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(DependencyBase.PARAM_PATH)
        cache = d.pop(OutputLOCAL.PARAM_CACHE, True)
        ret.append(_get(stage, p, info=d, cache=cache))
    return ret


def loads_from(stage, s_list, use_cache=True):
    ret = []
    for s in s_list:
        ret.append(_get(stage, s, info={}, cache=use_cache))
    return ret
