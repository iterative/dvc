import schema

from dvc.exceptions import DvcException
from dvc.config import Config

from dvc.dependency import SCHEMA, urlparse
from dvc.dependency.base import DependencyBase
from dvc.output.s3 import OutputS3
from dvc.output.gs import OutputGS
from dvc.output.local import OutputLOCAL
from dvc.output.hdfs import OutputHDFS
from dvc.output.ssh import OutputSSH

from dvc.remote import Remote


OUTS = [OutputHDFS, OutputS3, OutputGS, OutputSSH, OutputLOCAL]

OUTS_MAP = {'hdfs': OutputHDFS,
            's3': OutputS3,
            'gs': OutputGS,
            'ssh': OutputSSH,
            '': OutputLOCAL}

SCHEMA[schema.Optional(OutputLOCAL.PARAM_CACHE)] = bool
SCHEMA[schema.Optional(OutputLOCAL.PARAM_METRIC)] = OutputLOCAL.METRIC_SCHEMA


def _get(stage, p, info, cache, metric):
    parsed = urlparse(p)
    if parsed.scheme == 'remote':
        name = Config.SECTION_REMOTE_FMT.format(parsed.netloc)
        sect = stage.project.config._config[name]
        remote = Remote(stage.project, sect)
        return OUTS_MAP[remote.scheme](stage,
                                       p,
                                       info,
                                       cache=cache,
                                       remote=remote,
                                       metric=metric)

    for o in OUTS:
        if o.supported(p):
            return o(stage, p, info, cache=cache, remote=None, metric=metric)
    raise DvcException('Output \'{}\' is not supported'.format(p))


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(DependencyBase.PARAM_PATH)
        cache = d.pop(OutputLOCAL.PARAM_CACHE, True)
        metric = d.pop(OutputLOCAL.PARAM_METRIC, False)
        ret.append(_get(stage, p, info=d, cache=cache, metric=metric))
    return ret


def loads_from(stage, s_list, use_cache=True, metric=False):
    ret = []
    for s in s_list:
        ret.append(_get(stage, s, info={}, cache=use_cache, metric=metric))
    return ret
