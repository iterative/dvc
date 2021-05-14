from collections import defaultdict
from urllib.parse import urlparse

from funcy import collecting, project
from voluptuous import And, Any, Coerce, Length, Lower, Required, SetTo

from dvc.hash_info import HashInfo
from dvc.output.base import BaseOutput
from dvc.output.hdfs import HDFSOutput
from dvc.output.local import LocalOutput
from dvc.output.s3 import S3Output
from dvc.output.ssh import SSHOutput
from dvc.output.webhdfs import WebHDFSOutput
from dvc.scheme import Schemes

from ..fs import get_cloud_fs
from ..fs.hdfs import HDFSFileSystem
from ..fs.local import LocalFileSystem
from ..fs.s3 import S3FileSystem
from ..fs.webhdfs import WebHDFSFileSystem

OUTS_MAP = {
    Schemes.HDFS: HDFSOutput,
    Schemes.S3: S3Output,
    Schemes.SSH: SSHOutput,
    Schemes.LOCAL: LocalOutput,
    Schemes.WEBHDFS: WebHDFSOutput,
}

CHECKSUM_SCHEMA = Any(
    None,
    And(str, Length(max=0), SetTo(None)),
    And(Any(str, And(int, Coerce(str))), Length(min=3), Lower),
)

# NOTE: currently there are only 3 possible checksum names:
#
#    1) md5 (LOCAL, SSH);
#    2) etag (S3);
#    3) checksum (HDFS);
#
# so when a few types of outputs share the same name, we only need
# specify it once.
CHECKSUMS_SCHEMA = {
    LocalFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    S3FileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    HDFSFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
    WebHDFSFileSystem.PARAM_CHECKSUM: CHECKSUM_SCHEMA,
}

SCHEMA = CHECKSUMS_SCHEMA.copy()
SCHEMA[Required(BaseOutput.PARAM_PATH)] = str
SCHEMA[BaseOutput.PARAM_CACHE] = bool
SCHEMA[BaseOutput.PARAM_METRIC] = BaseOutput.METRIC_SCHEMA
SCHEMA[BaseOutput.PARAM_PLOT] = bool
SCHEMA[BaseOutput.PARAM_PERSIST] = bool
SCHEMA[BaseOutput.PARAM_CHECKPOINT] = bool
SCHEMA[HashInfo.PARAM_SIZE] = int
SCHEMA[HashInfo.PARAM_NFILES] = int
SCHEMA[BaseOutput.PARAM_DESC] = str
SCHEMA[BaseOutput.PARAM_ISEXEC] = bool


def _get(
    stage,
    p,
    info=None,
    cache=True,
    metric=False,
    plot=False,
    persist=False,
    checkpoint=False,
    live=False,
    desc=None,
    isexec=False,
):
    parsed = urlparse(p)

    if parsed.scheme == "remote":
        fs = get_cloud_fs(stage.repo, name=parsed.netloc)
        return OUTS_MAP[fs.scheme](
            stage,
            p,
            info,
            cache=cache,
            fs=fs,
            metric=metric,
            plot=plot,
            persist=persist,
            checkpoint=checkpoint,
            live=live,
            desc=desc,
            isexec=isexec,
        )

    out_cls = OUTS_MAP.get(parsed.scheme, LocalOutput)
    return out_cls(
        stage,
        p,
        info,
        cache=cache,
        fs=None,
        metric=metric,
        plot=plot,
        persist=persist,
        checkpoint=checkpoint,
        live=live,
        desc=desc,
        isexec=isexec,
    )


def loadd_from(stage, d_list):
    ret = []
    for d in d_list:
        p = d.pop(BaseOutput.PARAM_PATH)
        cache = d.pop(BaseOutput.PARAM_CACHE, True)
        metric = d.pop(BaseOutput.PARAM_METRIC, False)
        plot = d.pop(BaseOutput.PARAM_PLOT, False)
        persist = d.pop(BaseOutput.PARAM_PERSIST, False)
        checkpoint = d.pop(BaseOutput.PARAM_CHECKPOINT, False)
        desc = d.pop(BaseOutput.PARAM_DESC, False)
        isexec = d.pop(BaseOutput.PARAM_ISEXEC, False)
        live = d.pop(BaseOutput.PARAM_LIVE, False)
        ret.append(
            _get(
                stage,
                p,
                info=d,
                cache=cache,
                metric=metric,
                plot=plot,
                persist=persist,
                checkpoint=checkpoint,
                desc=desc,
                isexec=isexec,
                live=live,
            )
        )
    return ret


def loads_from(
    stage,
    s_list,
    use_cache=True,
    metric=False,
    plot=False,
    persist=False,
    checkpoint=False,
    isexec=False,
    live=False,
):
    return [
        _get(
            stage,
            s,
            info={},
            cache=use_cache,
            metric=metric,
            plot=plot,
            persist=persist,
            checkpoint=checkpoint,
            isexec=isexec,
            live=live,
        )
        for s in s_list
    ]


def _split_dict(d, keys):
    return project(d, keys), project(d, d.keys() - keys)


def _merge_data(s_list):
    d = defaultdict(dict)
    for key in s_list:
        if isinstance(key, str):
            d[key].update({})
            continue
        if not isinstance(key, dict):
            raise ValueError(f"'{type(key).__name__}' not supported.")

        for k, flags in key.items():
            if not isinstance(flags, dict):
                raise ValueError(
                    f"Expected dict for '{k}', got: '{type(flags).__name__}'"
                )
            d[k].update(flags)
    return d


@collecting
def load_from_pipeline(stage, data, typ="outs"):
    if typ not in (
        stage.PARAM_OUTS,
        stage.PARAM_METRICS,
        stage.PARAM_PLOTS,
        stage.PARAM_LIVE,
    ):
        raise ValueError(f"'{typ}' key is not allowed for pipeline files.")

    metric = typ == stage.PARAM_METRICS
    plot = typ == stage.PARAM_PLOTS
    live = typ == stage.PARAM_LIVE

    if live:
        # `live` is single object
        data = [data]

    d = _merge_data(data)

    for path, flags in d.items():
        plt_d, live_d = {}, {}
        if plot:
            from dvc.schema import PLOT_PROPS

            plt_d, flags = _split_dict(flags, keys=PLOT_PROPS.keys())
        if live:
            from dvc.schema import LIVE_PROPS

            live_d, flags = _split_dict(flags, keys=LIVE_PROPS.keys())
        extra = project(
            flags,
            [
                BaseOutput.PARAM_CACHE,
                BaseOutput.PARAM_PERSIST,
                BaseOutput.PARAM_CHECKPOINT,
            ],
        )

        yield _get(
            stage,
            path,
            {},
            plot=plt_d or plot,
            metric=metric,
            live=live_d or live,
            **extra,
        )
