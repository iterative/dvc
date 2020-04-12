from voluptuous import Any, Schema
from dvc import dependency
from dvc import output


class StageParams:
    PARAM_MD5 = "md5"
    PARAM_CMD = "cmd"
    PARAM_WDIR = "wdir"
    PARAM_DEPS = "deps"
    PARAM_OUTS = "outs"
    PARAM_LOCKED = "locked"
    PARAM_META = "meta"
    PARAM_ALWAYS_CHANGED = "always_changed"


SCHEMA = {
    StageParams.PARAM_MD5: output.CHECKSUM_SCHEMA,
    StageParams.PARAM_CMD: Any(str, None),
    StageParams.PARAM_WDIR: Any(str, None),
    StageParams.PARAM_DEPS: Any([dependency.SCHEMA], None),
    StageParams.PARAM_OUTS: Any([output.SCHEMA], None),
    StageParams.PARAM_LOCKED: bool,
    StageParams.PARAM_META: object,
    StageParams.PARAM_ALWAYS_CHANGED: bool,
}

SINGLE_STAGE_SCHEMA = Schema(SCHEMA)
