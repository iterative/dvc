from dvc.stage.params import StageParams, OutputParams
from dvc.output import CHECKSUMS_SCHEMA
from dvc import dependency, output

from voluptuous import Any, Schema, Optional, Required


STAGES = "stages"
SINGLE_STAGE_SCHEMA = {
    StageParams.PARAM_MD5: output.CHECKSUM_SCHEMA,
    StageParams.PARAM_CMD: Any(str, None),
    StageParams.PARAM_WDIR: Any(str, None),
    StageParams.PARAM_DEPS: Any([dependency.SCHEMA], None),
    StageParams.PARAM_OUTS: Any([output.SCHEMA], None),
    StageParams.PARAM_LOCKED: bool,
    StageParams.PARAM_META: object,
    StageParams.PARAM_ALWAYS_CHANGED: bool,
}

DATA_SCHEMA = {**CHECKSUMS_SCHEMA, Required("path"): str}
LOCK_FILE_STAGE_SCHEMA = {
    Required(StageParams.PARAM_CMD): str,
    StageParams.PARAM_DEPS: [DATA_SCHEMA],
    StageParams.PARAM_PARAMS: {str: {str: object}},
    StageParams.PARAM_OUTS: [DATA_SCHEMA],
}
LOCKFILE_SCHEMA = {str: LOCK_FILE_STAGE_SCHEMA}

SINGLE_PIPELINE_STAGE_SCHEMA = {
    str: {
        StageParams.PARAM_CMD: str,
        Optional(StageParams.PARAM_WDIR): str,
        Optional(StageParams.PARAM_DEPS): [str],
        Optional(StageParams.PARAM_PARAMS): [Any(str, {str: [str]})],
        Optional(StageParams.PARAM_LOCKED): bool,
        Optional(StageParams.PARAM_META): object,
        Optional(StageParams.PARAM_ALWAYS_CHANGED): bool,
        **{Optional(p.value): [str] for p in OutputParams},
    }
}
MULTI_STAGE_SCHEMA = {STAGES: SINGLE_PIPELINE_STAGE_SCHEMA}

COMPILED_SINGLE_STAGE_SCHEMA = Schema(SINGLE_STAGE_SCHEMA)
COMPILED_MULTI_STAGE_SCHEMA = Schema(MULTI_STAGE_SCHEMA)
COMPILED_LOCK_FILE_STAGE_SCHEMA = Schema(LOCK_FILE_STAGE_SCHEMA)
COMPILED_LOCKFILE_SCHEMA = Schema(LOCKFILE_SCHEMA)
