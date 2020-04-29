from urllib.parse import urlparse
from voluptuous import Any, Required, Lower, Length, Coerce, And, SetTo

from dvc.output.base import BaseOutput
from dvc.output.gs import GSOutput
from dvc.output.hdfs import HDFSOutput
from dvc.output.local import LocalOutput
from dvc.output.s3 import S3Output
from dvc.output.ssh import SSHOutput
from dvc.remote import Remote
from dvc.remote.hdfs import HDFSRemote
from dvc.remote.local import LocalRemote
from dvc.remote.s3 import S3Remote
from dvc.scheme import Schemes

OUTS = [
    HDFSOutput,
    S3Output,
    GSOutput,
    SSHOutput,
    # NOTE: LocalOutput is the default choice
]

OUTS_MAP = {
    Schemes.HDFS: HDFSOutput,
    Schemes.S3: S3Output,
    Schemes.GS: GSOutput,
    Schemes.SSH: SSHOutput,
    Schemes.LOCAL: LocalOutput,
}

CHECKSUM_SCHEMA = Any(
    None,
    And(str, Length(max=0), SetTo(None)),
    And(Any(str, And(int, Coerce(str))), Length(min=3), Lower),
)

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH, GS);
#    2) etag (S3);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
CHECKSUMS_SCHEMA = {
    LocalRemote.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    S3Remote.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    HDFSRemote.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
}

SCHEMA = CHECKSUMS_SCHEMA.copy()
SCHEMA[Required(BaseOutput.PARAM_PATH)] = str
SCHEMA[BaseOutput.PARAM_CACHE] = bool
SCHEMA[BaseOutput.PARAM_METRIC] = BaseOutput.METRIC_SCHEMA
SCHEMA[BaseOutput.PARAM_PERSIST] = bool


def _get(stage, p, info, cache, metric, persist=False):
    parsed = urlparse(p)

    if parsed.scheme == "remote":
        remote = Remote(stage.repo, name=parsed.netloc)
        return OUTS_MAP[remote.scheme](
            stage,
            p,
            info,
            cache=cache,
            remote=remote,
            metric=metric,
            persist=persist,
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
            )
    return LocalOutput(
        stage,
        p,
        info,
        cache=cache,
        remote=None,
        metric=metric,
        persist=persist,
    )


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(BaseOutput.PARAM_PATH)
        cache = d.pop(BaseOutput.PARAM_CACHE, True)
        metric = d.pop(BaseOutput.PARAM_METRIC, False)
        persist = d.pop(BaseOutput.PARAM_PERSIST, False)
        ret.append(
            _get(
                stage, p, info=d, cache=cache, metric=metric, persist=persist,
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
