from __future__ import unicode_literals

import schema

from dvc.path import Schemes
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
    Schemes.HDFS: OutputHDFS,
    Schemes.S3: OutputS3,
    Schemes.GS: OutputGS,
    Schemes.SSH: OutputSSH,
    Schemes.LOCAL: OutputLOCAL,
}

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH, GS);
#    2) etag (S3);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
CHECKSUM_SCHEMA = {
    schema.Optional(RemoteLOCAL.PARAM_CHECKSUM): schema.Or(str, None),
    schema.Optional(RemoteS3.PARAM_CHECKSUM): schema.Or(str, None),
    schema.Optional(RemoteHDFS.PARAM_CHECKSUM): schema.Or(str, None),
}

TAGS_SCHEMA = {schema.Optional(str): CHECKSUM_SCHEMA}

SCHEMA = CHECKSUM_SCHEMA.copy()
SCHEMA[OutputBase.PARAM_PATH] = str
SCHEMA[schema.Optional(OutputBase.PARAM_CACHE)] = bool
SCHEMA[schema.Optional(OutputBase.PARAM_METRIC)] = OutputBase.METRIC_SCHEMA
SCHEMA[schema.Optional(OutputBase.PARAM_TAGS)] = TAGS_SCHEMA
SCHEMA[schema.Optional(OutputBase.PARAM_PERSIST)] = bool


def _get(stage, p, info, cache, metric, persist=False, tags=None):
    parsed = urlparse(p)

    if parsed.scheme == "remote":
        settings = stage.repo.config.get_remote_settings(parsed.netloc)
        remote = Remote(stage.repo, settings)
        return OUTS_MAP[remote.scheme](
            stage,
            p,
            info,
            cache=cache,
            remote=remote,
            metric=metric,
            persist=persist,
            tags=tags,
        )

    for o in OUTS:
        if o.supported(p):
            return o(
                stage,
                p,
                info,
                cache=cache,
                remote=None,
                metric=metric,
                persist=persist,
                tags=tags,
            )
    return OutputLOCAL(
        stage,
        p,
        info,
        cache=cache,
        remote=None,
        metric=metric,
        persist=persist,
        tags=tags,
    )


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(OutputBase.PARAM_PATH)
        cache = d.pop(OutputBase.PARAM_CACHE, True)
        metric = d.pop(OutputBase.PARAM_METRIC, False)
        persist = d.pop(OutputBase.PARAM_PERSIST, False)
        tags = d.pop(OutputBase.PARAM_TAGS, None)
        ret.append(
            _get(
                stage,
                p,
                info=d,
                cache=cache,
                metric=metric,
                persist=persist,
                tags=tags,
            )
        )
    return ret


def loads_from(stage, s_list, use_cache=True, metric=False, persist=False):
    ret = []
    for s in s_list:
        ret.append(
            _get(
                stage,
                s,
                info={},
                cache=use_cache,
                metric=metric,
                persist=persist,
            )
        )
    return ret
