from typing import TYPE_CHECKING

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


def to_pipeline_file(stage: "PipelineStage"):
    return {
        stage.name: {
            key: value
            for key, value in {
                stage.PARAM_CMD: stage.cmd,
                stage.PARAM_WDIR: stage.resolve_wdir(),
                stage.PARAM_DEPS: [d.def_path for d in stage.deps],
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
    deps = [
        {"path": dep.def_path, dep.checksum_type: dep.get_checksum()}
        for dep in stage.deps
    ]
    outs = [
        {"path": out.def_path, out.checksum_type: out.get_checksum()}
        for out in stage.outs
    ]
    if stage.deps:
        res["deps"] = deps
    if stage.outs:
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
