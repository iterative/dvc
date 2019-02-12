from __future__ import unicode_literals

import schema

from dvc.config import Config
from dvc.utils.compat import urlparse, str

from dvc.output.base import OutputBase
from dvc.output.s3 import OutputS3
from dvc.output.gs import OutputGS
from dvc.output.local import OutputLOCAL
from dvc.output.hdfs import OutputHDFS
from dvc.output.ssh import OutputSSH

from dvc.remote import Remote
from dvc.remote.s3 import RemoteS3
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.local import RemoteLOCAL

OUTS = [
    OutputHDFS,
    OutputS3,
    OutputGS,
    OutputSSH,
    # NOTE: OutputLOCAL is the default choice
]

OUTS_MAP = {
    "hdfs": OutputHDFS,
    "s3": OutputS3,
    "gs": OutputGS,
    "ssh": OutputSSH,
    "local": OutputLOCAL,
}

SCHEMA = {
    OutputBase.PARAM_PATH: str,
    # NOTE: currently there are only 3 possible checksum names:
    #
    #    1) md5 (LOCAL, SSH, GS);
    #    2) etag (S3);
    #    3) checksum (HDFS);
    #
    # so when a few types of outputs share the same name, we only need
    # specify it once.
    schema.Optional(RemoteLOCAL.PARAM_CHECKSUM): schema.Or(str, None),
    schema.Optional(RemoteS3.PARAM_CHECKSUM): schema.Or(str, None),
    schema.Optional(RemoteHDFS.PARAM_CHECKSUM): schema.Or(str, None),
    schema.Optional(OutputBase.PARAM_CACHE): bool,
    schema.Optional(OutputBase.PARAM_METRIC): OutputBase.METRIC_SCHEMA,
}


def _get(stage, p, info, cache, metric):
    parsed = urlparse(p)
    if parsed.scheme == "remote":
        name = Config.SECTION_REMOTE_FMT.format(parsed.netloc)
        sect = stage.repo.config.config[name]
        remote = Remote(stage.repo, sect)
        return OUTS_MAP[remote.scheme](
            stage, p, info, cache=cache, remote=remote, metric=metric
        )

    for o in OUTS:
        if o.supported(p):
            return o(stage, p, info, cache=cache, remote=None, metric=metric)
    return OutputLOCAL(stage, p, info, cache=cache, remote=None, metric=metric)


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(OutputBase.PARAM_PATH)
        cache = d.pop(OutputBase.PARAM_CACHE, True)
        metric = d.pop(OutputBase.PARAM_METRIC, False)
        ret.append(_get(stage, p, info=d, cache=cache, metric=metric))
    return ret


def loads_from(stage, s_list, use_cache=True, metric=False):
    ret = []
    for s in s_list:
        ret.append(_get(stage, s, info={}, cache=use_cache, metric=metric))
    return ret
