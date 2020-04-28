from typing import TYPE_CHECKING

from funcy import rpartial, lsplit_by

from dvc.dependency import ParamsDependency
from dvc.utils.collections import apply_diff
from dvc.utils.stage import parse_stage_for_update

if TYPE_CHECKING:
    from dvc.stage import PipelineStage, Stage


def _get_outs(stage: "PipelineStage"):
    outs_bucket = {}
    for o in stage.outs:
        bucket_key = ["metrics"] if o.metric else ["outs"]

        if not o.metric and o.persist:
            bucket_key += ["persist"]
        if not o.use_cache:
            bucket_key += ["no_cache"]
        key = "_".join(bucket_key)
        outs_bucket[key] = outs_bucket.get(key, []) + [o.def_path]
    return outs_bucket


def get_params_deps(stage):
    params, deps = lsplit_by(
        rpartial(isinstance, ParamsDependency), stage.deps
    )
    params_keys = []
    params_ = []
    for param in params:
        dump = param.dumpd()
        path, param_ = dump["path"], dump[stage.PARAM_PARAMS]
        param_keys = list(param_.keys())
        if path == ParamsDependency.DEFAULT_PARAMS_FILE:
            params_keys += param_keys
        else:
            params_keys += [{path: param_keys}] if param_keys else []
        params_ += [{path: dump[stage.PARAM_PARAMS]}]
    return params_keys, params_, deps


def to_pipeline_file(stage: "PipelineStage"):
    params, _, deps = get_params_deps(stage)
    return {
        stage.name: {
            key: value
            for key, value in {
                stage.PARAM_CMD: stage.cmd,
                stage.PARAM_WDIR: stage.resolve_wdir(),
                stage.PARAM_DEPS: deps,
                stage.PARAM_PARAMS: params,
                **_get_outs(stage),
                stage.PARAM_LOCKED: stage.locked,
                stage.PARAM_ALWAYS_CHANGED: stage.always_changed,
            }.items()
            if value
        }
    }


def to_lockfile(stage: "PipelineStage") -> dict:
    assert stage.cmd
    assert stage.name

    res = {"cmd": stage.cmd}
    _, params, deps = get_params_deps(stage)
    deps = [
        {"path": dep.def_path, dep.checksum_type: dep.get_checksum()}
        for dep in deps
    ]
    outs = [
        {"path": out.def_path, out.checksum_type: out.get_checksum()}
        for out in stage.outs
    ]
    if deps:
        res["deps"] = deps
    if params:
        res["params"] = params
    if outs:
        res["outs"] = outs

    return {stage.name: res}


def to_single_stage_file(stage: "Stage"):
    state = stage.dumpd()

    # When we load a stage we parse yaml with a fast parser, which strips
    # off all the comments and formatting. To retain those on update we do
    # a trick here:
    # - reparse the same yaml text with a slow but smart ruamel yaml parser
    # - apply changes to a returned structure
    # - serialize it
    if stage._stage_text is not None:
        saved_state = parse_stage_for_update(stage._stage_text, stage.path)
        # Stage doesn't work with meta in any way, so .dumpd() doesn't
        # have it. We simply copy it over.
        if "meta" in saved_state:
            state["meta"] = saved_state["meta"]
        apply_diff(state, saved_state)
        state = saved_state
    return state
