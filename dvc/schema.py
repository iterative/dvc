from collections.abc import Mapping
from typing import Any, Dict

import voluptuous as vol

from dvc import dependency, output
from dvc.annotations import ANNOTATION_SCHEMA, ARTIFACT_SCHEMA
from dvc.output import (
    CHECKSUMS_SCHEMA,
    CLOUD_SCHEMA,
    DIR_FILES_SCHEMA,
    META_SCHEMA,
    Output,
)
from dvc.parsing import DO_KWD, FOREACH_KWD, MATRIX_KWD, VARS_KWD
from dvc.stage.params import StageParams

STAGES = "stages"
SINGLE_STAGE_SCHEMA = {
    StageParams.PARAM_MD5: output.CHECKSUM_SCHEMA,
    StageParams.PARAM_WDIR: vol.Any(str, None),
    StageParams.PARAM_DEPS: vol.Any([dependency.SCHEMA], None),
    StageParams.PARAM_OUTS: vol.Any([output.SCHEMA], None),
    StageParams.PARAM_LOCKED: bool,  # backward compatibility
    StageParams.PARAM_FROZEN: bool,
    StageParams.PARAM_META: object,
    StageParams.PARAM_ALWAYS_CHANGED: bool,
    StageParams.PARAM_DESC: str,
}

DATA_SCHEMA: Dict[Any, Any] = {
    **CHECKSUMS_SCHEMA,
    **META_SCHEMA,
    vol.Required("path"): str,
    Output.PARAM_CLOUD: CLOUD_SCHEMA,
    Output.PARAM_FILES: [DIR_FILES_SCHEMA],
    Output.PARAM_HASH: str,
}
LOCK_FILE_STAGE_SCHEMA = {
    vol.Required(StageParams.PARAM_CMD): vol.Any(str, list),
    StageParams.PARAM_DEPS: [DATA_SCHEMA],
    StageParams.PARAM_PARAMS: {str: {str: object}},
    StageParams.PARAM_OUTS: [DATA_SCHEMA],
}

LOCKFILE_STAGES_SCHEMA = {str: LOCK_FILE_STAGE_SCHEMA}
LOCKFILE_SCHEMA = {
    vol.Required("schema"): vol.Equal("2.0", "invalid schema version"),
    STAGES: LOCKFILE_STAGES_SCHEMA,
}

OUT_PSTAGE_DETAILED_SCHEMA = {
    str: {
        **ANNOTATION_SCHEMA,  # type: ignore[arg-type]
        Output.PARAM_CACHE: bool,
        Output.PARAM_PERSIST: bool,
        "checkpoint": bool,
        Output.PARAM_REMOTE: str,
        Output.PARAM_PUSH: bool,
    }
}

PLOTS = "plots"
PLOT_PROPS = {
    Output.PARAM_PLOT_TEMPLATE: str,
    Output.PARAM_PLOT_X: str,
    Output.PARAM_PLOT_Y: str,
    Output.PARAM_PLOT_X_LABEL: str,
    Output.PARAM_PLOT_Y_LABEL: str,
    Output.PARAM_PLOT_TITLE: str,
    Output.PARAM_PLOT_HEADER: bool,
}
PLOT_PROPS_SCHEMA = {**OUT_PSTAGE_DETAILED_SCHEMA[str], **PLOT_PROPS}
PLOT_PSTAGE_SCHEMA = {str: vol.Any(PLOT_PROPS_SCHEMA, [PLOT_PROPS_SCHEMA])}

PARAM_PSTAGE_NON_DEFAULT_SCHEMA = {str: [str]}

VARS_SCHEMA = [str, dict]

STAGE_DEFINITION = {
    MATRIX_KWD: {str: vol.Any(str, list)},
    vol.Required(StageParams.PARAM_CMD): vol.Any(str, list),
    vol.Optional(StageParams.PARAM_WDIR): str,
    vol.Optional(StageParams.PARAM_DEPS): [str],
    vol.Optional(StageParams.PARAM_PARAMS): [vol.Any(str, dict)],
    vol.Optional(VARS_KWD): VARS_SCHEMA,
    vol.Optional(StageParams.PARAM_FROZEN): bool,
    vol.Optional(StageParams.PARAM_META): object,
    vol.Optional(StageParams.PARAM_DESC): str,
    vol.Optional(StageParams.PARAM_ALWAYS_CHANGED): bool,
    vol.Optional(StageParams.PARAM_OUTS): [vol.Any(str, OUT_PSTAGE_DETAILED_SCHEMA)],
    vol.Optional(StageParams.PARAM_METRICS): [vol.Any(str, OUT_PSTAGE_DETAILED_SCHEMA)],
    vol.Optional(StageParams.PARAM_PLOTS): [vol.Any(str, PLOT_PSTAGE_SCHEMA)],
}


def either_or(primary, fallback, fallback_includes=None):
    def validator(data):
        schema = primary
        if isinstance(data, Mapping) and set(fallback_includes or []) & data.keys():
            schema = fallback
        return vol.Schema(schema)(data)

    return validator


PLOT_DEFINITION = {
    Output.PARAM_PLOT_X: vol.Any(str, {str: str}),
    Output.PARAM_PLOT_Y: vol.Any(str, [str], {str: vol.Any(str, [str])}),
    Output.PARAM_PLOT_X_LABEL: str,
    Output.PARAM_PLOT_Y_LABEL: str,
    Output.PARAM_PLOT_TITLE: str,
    Output.PARAM_PLOT_TEMPLATE: str,
}
SINGLE_PLOT_SCHEMA = {vol.Required(str): vol.Any(PLOT_DEFINITION, None)}
ARTIFACTS = "artifacts"
SINGLE_ARTIFACT_SCHEMA = vol.Schema({str: ARTIFACT_SCHEMA})
FOREACH_IN = {
    vol.Required(FOREACH_KWD): vol.Any(dict, list, str),
    vol.Required(DO_KWD): STAGE_DEFINITION,
}
SINGLE_PIPELINE_STAGE_SCHEMA = {
    str: either_or(STAGE_DEFINITION, FOREACH_IN, [FOREACH_KWD, DO_KWD])
}
MULTI_STAGE_SCHEMA = {
    PLOTS: [vol.Any(str, SINGLE_PLOT_SCHEMA)],
    STAGES: SINGLE_PIPELINE_STAGE_SCHEMA,
    VARS_KWD: VARS_SCHEMA,
    StageParams.PARAM_PARAMS: [str],
    StageParams.PARAM_METRICS: [str],
    ARTIFACTS: SINGLE_ARTIFACT_SCHEMA,
}

COMPILED_SINGLE_STAGE_SCHEMA = vol.Schema(SINGLE_STAGE_SCHEMA)
COMPILED_MULTI_STAGE_SCHEMA = vol.Schema(MULTI_STAGE_SCHEMA)
COMPILED_LOCK_FILE_STAGE_SCHEMA = vol.Schema(LOCK_FILE_STAGE_SCHEMA)
COMPILED_LOCKFILE_SCHEMA = vol.Schema(LOCKFILE_SCHEMA)
