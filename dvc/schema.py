from voluptuous import Any, Optional, Required, Schema

from dvc import dependency, output
from dvc.output import CHECKSUMS_SCHEMA, BaseOutput
from dvc.stage.params import StageParams

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

OUT_PSTAGE_DETAILED_SCHEMA = {
    str: {BaseOutput.PARAM_CACHE: bool, BaseOutput.PARAM_PERSIST: bool}
}

PLOT_PSTAGE_SCHEMA = OUT_PSTAGE_DETAILED_SCHEMA.copy()
PLOT_PSTAGE_SCHEMA[str][BaseOutput.PARAM_PLOT_TEMPLATE] = str

PARAM_PSTAGE_NON_DEFAULT_SCHEMA = {str: [str]}

SINGLE_PIPELINE_STAGE_SCHEMA = {
    str: {
        StageParams.PARAM_CMD: str,
        Optional(StageParams.PARAM_WDIR): str,
        Optional(StageParams.PARAM_DEPS): [str],
        Optional(StageParams.PARAM_PARAMS): [
            Any(str, PARAM_PSTAGE_NON_DEFAULT_SCHEMA)
        ],
        Optional(StageParams.PARAM_LOCKED): bool,
        Optional(StageParams.PARAM_META): object,
        Optional(StageParams.PARAM_ALWAYS_CHANGED): bool,
        Optional(StageParams.PARAM_OUTS): [
            Any(str, OUT_PSTAGE_DETAILED_SCHEMA)
        ],
        Optional(StageParams.PARAM_METRICS): [
            Any(str, OUT_PSTAGE_DETAILED_SCHEMA)
        ],
        Optional(StageParams.PARAM_PLOTS): [Any(str, PLOT_PSTAGE_SCHEMA)],
    }
}
MULTI_STAGE_SCHEMA = {STAGES: SINGLE_PIPELINE_STAGE_SCHEMA}

COMPILED_SINGLE_STAGE_SCHEMA = Schema(SINGLE_STAGE_SCHEMA)
COMPILED_MULTI_STAGE_SCHEMA = Schema(MULTI_STAGE_SCHEMA)
COMPILED_LOCK_FILE_STAGE_SCHEMA = Schema(LOCK_FILE_STAGE_SCHEMA)
COMPILED_LOCKFILE_SCHEMA = Schema(LOCKFILE_SCHEMA)
