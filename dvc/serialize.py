from collections import OrderedDict
from functools import partial
from operator import attrgetter
from typing import TYPE_CHECKING, List

from funcy import lsplit, rpartial

from dvc.dependency import ParamsDependency
from dvc.output import BaseOutput
from dvc.stage.params import StageParams
from dvc.stage.utils import resolve_wdir
from dvc.utils.collections import apply_diff
from dvc.utils.stage import parse_stage_for_update

if TYPE_CHECKING:
    from dvc.stage import PipelineStage, Stage

PARAM_PARAMS = ParamsDependency.PARAM_PARAMS
PARAM_PATH = ParamsDependency.PARAM_PATH

PARAM_DEPS = StageParams.PARAM_DEPS
PARAM_OUTS = StageParams.PARAM_OUTS

PARAM_CACHE = BaseOutput.PARAM_CACHE
PARAM_METRIC = BaseOutput.PARAM_METRIC
PARAM_PLOT = BaseOutput.PARAM_PLOT
PARAM_PERSIST = BaseOutput.PARAM_PERSIST

DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE


sort_by_path = partial(sorted, key=attrgetter("def_path"))


def _get_out(out):
    res = OrderedDict()
    if not out.use_cache:
        res[PARAM_CACHE] = False
    if out.persist:
        res[PARAM_PERSIST] = True
    if out.plot and isinstance(out.plot, dict):
        res.update(out.plot)
    return out.def_path if not res else {out.def_path: res}


def _get_outs(outs):
    return [_get_out(out) for out in sort_by_path(outs)]


def get_params_deps(stage: "PipelineStage"):
    return lsplit(rpartial(isinstance, ParamsDependency), stage.deps)


def _serialize_params(params: List[ParamsDependency]):
    """Return two types of values from stage:

    `keys` - which is list of params without values, used in a pipeline file

    which is in the shape of:
        ['lr', 'train', {'params2.yaml': ['lr']}]

    `key_vals` - which is list of params with values, used in a lockfile
    which is in the shape of:
        {'params.yaml': {'lr': '1', 'train': 2}, {'params2.yaml': {'lr': '1'}}
    """
    keys = []
    key_vals = OrderedDict()

    for param_dep in sort_by_path(params):
        dump = param_dep.dumpd()
        path, params = dump[PARAM_PATH], dump[PARAM_PARAMS]
        k = list(params.keys())
        if not k:
            continue
        key_vals[path] = OrderedDict([(key, params[key]) for key in sorted(k)])
        # params from default file is always kept at the start of the `params:`
        if path == DEFAULT_PARAMS_FILE:
            keys = k + keys
            key_vals.move_to_end(path, last=False)
        else:
            # if it's not a default file, change the shape
            # to: {path: k}
            keys.append({path: k})
    return keys, key_vals


def to_pipeline_file(stage: "PipelineStage"):
    params, deps = get_params_deps(stage)
    serialized_params, _ = _serialize_params(params)

    res = [
        (stage.PARAM_CMD, stage.cmd),
        (stage.PARAM_WDIR, resolve_wdir(stage.wdir, stage.path)),
        (stage.PARAM_DEPS, sorted([d.def_path for d in deps])),
        (stage.PARAM_PARAMS, serialized_params),
        (
            PARAM_OUTS,
            _get_outs(
                [out for out in stage.outs if not (out.metric or out.plot)]
            ),
        ),
        (
            stage.PARAM_METRICS,
            _get_outs([out for out in stage.outs if out.metric]),
        ),
        (
            stage.PARAM_PLOTS,
            _get_outs([out for out in stage.outs if out.plot]),
        ),
        (stage.PARAM_LOCKED, stage.locked),
        (stage.PARAM_ALWAYS_CHANGED, stage.always_changed),
    ]
    return {
        stage.name: OrderedDict([(key, value) for key, value in res if value])
    }


def to_single_stage_lockfile(stage: "Stage") -> dict:
    assert stage.cmd

    res = OrderedDict([("cmd", stage.cmd)])
    params, deps = get_params_deps(stage)
    deps, outs = [
        [
            OrderedDict(
                [
                    (PARAM_PATH, item.def_path),
                    (item.checksum_type, item.checksum),
                ]
            )
            for item in sort_by_path(items)
        ]
        for items in [deps, stage.outs]
    ]
    if deps:
        res[PARAM_DEPS] = deps
    if params:
        _, res[PARAM_PARAMS] = _serialize_params(params)
    if outs:
        res[PARAM_OUTS] = outs

    return res


def to_lockfile(stage: "PipelineStage") -> dict:
    assert stage.name
    return {stage.name: to_single_stage_lockfile(stage)}


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
