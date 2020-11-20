from voluptuous import Any, Optional, Required, Schema

from dvc import dependency, output
from dvc.hash_info import HashInfo
from dvc.output import CHECKSUMS_SCHEMA, BaseOutput
from dvc.parsing import DO_KWD, FOREACH_KWD, SET_KWD, VARS_KWD
from dvc.stage.params import StageParams

STAGES = "stages"
SINGLE_STAGE_SCHEMA = {
    StageParams.PARAM_MD5: output.CHECKSUM_SCHEMA,
    StageParams.PARAM_CMD: Any(str, None),
    StageParams.PARAM_WDIR: Any(str, None),
    StageParams.PARAM_DEPS: Any([dependency.SCHEMA], None),
    StageParams.PARAM_OUTS: Any([output.SCHEMA], None),
    StageParams.PARAM_LOCKED: bool,  # backard compatibility
    StageParams.PARAM_FROZEN: bool,
    StageParams.PARAM_META: object,
    StageParams.PARAM_ALWAYS_CHANGED: bool,
    StageParams.PARAM_DESC: str,
}

DATA_SCHEMA = {
    **CHECKSUMS_SCHEMA,
    Required("path"): str,
    HashInfo.PARAM_SIZE: int,
    HashInfo.PARAM_NFILES: int,
}
LOCK_FILE_STAGE_SCHEMA = {
    Required(StageParams.PARAM_CMD): str,
    StageParams.PARAM_DEPS: [DATA_SCHEMA],
    StageParams.PARAM_PARAMS: {str: {str: object}},
    StageParams.PARAM_OUTS: [DATA_SCHEMA],
}
LOCKFILE_SCHEMA = {str: LOCK_FILE_STAGE_SCHEMA}

OUT_PSTAGE_DETAILED_SCHEMA = {
    str: {
        BaseOutput.PARAM_CACHE: bool,
        BaseOutput.PARAM_PERSIST: bool,
        BaseOutput.PARAM_CHECKPOINT: bool,
        BaseOutput.PARAM_DESC: str,
    }
}

PLOT_PROPS = {
    BaseOutput.PARAM_PLOT_TEMPLATE: str,
    BaseOutput.PARAM_PLOT_X: str,
    BaseOutput.PARAM_PLOT_Y: str,
    BaseOutput.PARAM_PLOT_X_LABEL: str,
    BaseOutput.PARAM_PLOT_Y_LABEL: str,
    BaseOutput.PARAM_PLOT_TITLE: str,
    BaseOutput.PARAM_PLOT_HEADER: bool,
}
PLOT_PROPS_SCHEMA = {
    **OUT_PSTAGE_DETAILED_SCHEMA[str],
    **PLOT_PROPS,
}
PLOT_PSTAGE_SCHEMA = {str: Any(PLOT_PROPS_SCHEMA, [PLOT_PROPS_SCHEMA])}

PARAM_PSTAGE_NON_DEFAULT_SCHEMA = {str: [str]}

VARS_SCHEMA = [str, dict]

STAGE_DEFINITION = {
    StageParams.PARAM_CMD: str,
    Optional(SET_KWD): dict,
    Optional(StageParams.PARAM_WDIR): str,
    Optional(StageParams.PARAM_DEPS): [str],
    Optional(StageParams.PARAM_PARAMS): [Any(str, dict)],
    Optional(VARS_KWD): VARS_SCHEMA,
    Optional(StageParams.PARAM_FROZEN): bool,
    Optional(StageParams.PARAM_META): object,
    Optional(StageParams.PARAM_DESC): str,
    Optional(StageParams.PARAM_ALWAYS_CHANGED): bool,
    Optional(StageParams.PARAM_OUTS): [Any(str, OUT_PSTAGE_DETAILED_SCHEMA)],
    Optional(StageParams.PARAM_METRICS): [
        Any(str, OUT_PSTAGE_DETAILED_SCHEMA)
    ],
    Optional(StageParams.PARAM_PLOTS): [Any(str, PLOT_PSTAGE_SCHEMA)],
}

FOREACH_IN = {
    Optional(SET_KWD): dict,
    Required(FOREACH_KWD): Any(dict, list, str),
    Required(DO_KWD): STAGE_DEFINITION,
}
SINGLE_PIPELINE_STAGE_SCHEMA = {str: Any(STAGE_DEFINITION, FOREACH_IN)}
MULTI_STAGE_SCHEMA = {
    STAGES: SINGLE_PIPELINE_STAGE_SCHEMA,
    VARS_KWD: VARS_SCHEMA,
}

COMPILED_SINGLE_STAGE_SCHEMA = Schema(SINGLE_STAGE_SCHEMA)
COMPILED_MULTI_STAGE_SCHEMA = Schema(MULTI_STAGE_SCHEMA)
COMPILED_LOCK_FILE_STAGE_SCHEMA = Schema(LOCK_FILE_STAGE_SCHEMA)
COMPILED_LOCKFILE_SCHEMA = Schema(LOCKFILE_SCHEMA)
