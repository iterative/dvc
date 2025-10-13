from collections import OrderedDict
from collections.abc import Iterable
from operator import attrgetter
from typing import TYPE_CHECKING, Any, Optional, Union, no_type_check

from funcy import post_processing

from dvc.dependency import ParamsDependency
from dvc.output import Annotation, Output
from dvc.utils.collections import apply_diff
from dvc.utils.serialize import parse_yaml_for_update

from .params import StageParams
from .utils import resolve_wdir, split_params_deps

if TYPE_CHECKING:
    from dvc.stage import PipelineStage, Stage

PARAM_PARAMS = ParamsDependency.PARAM_PARAMS
PARAM_PATH = ParamsDependency.PARAM_PATH

PARAM_DEPS = StageParams.PARAM_DEPS
PARAM_OUTS = StageParams.PARAM_OUTS

PARAM_CACHE = Output.PARAM_CACHE
PARAM_METRIC = Output.PARAM_METRIC
PARAM_PLOT = Output.PARAM_PLOT
PARAM_PERSIST = Output.PARAM_PERSIST
PARAM_DESC = Annotation.PARAM_DESC
PARAM_REMOTE = Output.PARAM_REMOTE
PARAM_PUSH = Output.PARAM_PUSH

DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE


@post_processing(OrderedDict)
def _get_flags(out):
    annot = out.annot.to_dict()
    yield from annot.items()

    if not out.use_cache:
        yield PARAM_CACHE, False
    if out.persist:
        yield PARAM_PERSIST, True
    if out.plot and isinstance(out.plot, dict):
        # notice `out.plot` is not sorted
        # `out.plot` is in the same order as is in the file when read
        # and, should be dumped as-is without any sorting
        yield from out.plot.items()
    if out.remote:
        yield PARAM_REMOTE, out.remote
    if not out.can_push:
        yield PARAM_PUSH, False


def _serialize_out(out):
    flags = _get_flags(out)
    return out.def_path if not flags else {out.def_path: flags}


@no_type_check
def _serialize_outs(outputs: list[Output]):
    outs, metrics, plots = [], [], []
    for out in sorted(outputs, key=attrgetter("def_path")):
        bucket = outs
        if out.plot:
            bucket = plots
        elif out.metric:
            bucket = metrics
        bucket.append(_serialize_out(out))
    return outs, metrics, plots


def _serialize_params_keys(params: Iterable["ParamsDependency"]):
    """
    Returns the following format of data:
     ['lr', 'train', {'params2.yaml': ['lr']}]

    The output is sorted, with keys of params from default params file being
    at the first, and then followed by entry of other files in lexicographic
    order. The keys of those custom files are also sorted in the same order.
    """
    keys: list[Union[str, dict[str, Optional[list[str]]]]] = []
    for param_dep in sorted(params, key=attrgetter("def_path")):
        # when on no_exec, params are not filled and are saved as list
        k: list[str] = sorted(param_dep.params)
        if k and param_dep.def_path == DEFAULT_PARAMS_FILE:
            keys = k + keys  # type: ignore[operator,assignment]
        else:
            keys.append({param_dep.def_path: k or None})
    return keys


@no_type_check
def _serialize_params_values(params: list[ParamsDependency]):
    """Returns output of following format, used for lockfile:
        {'params.yaml': {'lr': '1', 'train': 2}, {'params2.yaml': {'lr': '1'}}

    Default params file are always kept at the start, followed by others in
    alphabetical order. The param values are sorted too(not recursively though)
    """
    key_vals = OrderedDict()
    for param_dep in sorted(params, key=attrgetter("def_path")):
        dump = param_dep.dumpd()
        path, params = dump[PARAM_PATH], dump[PARAM_PARAMS]
        if isinstance(params, dict):
            kv = [(key, params[key]) for key in sorted(params.keys())]
            key_vals[path] = OrderedDict(kv)
            if path == DEFAULT_PARAMS_FILE:
                key_vals.move_to_end(path, last=False)
    return key_vals


def to_pipeline_file(stage: "PipelineStage"):
    wdir = resolve_wdir(stage.wdir, stage.path)
    param_objs, deps_objs = split_params_deps(stage)
    deps = sorted(d.def_path for d in deps_objs)
    params = _serialize_params_keys(param_objs)

    outs, metrics, plots = _serialize_outs(stage.outs)

    cmd = stage.cmd
    assert cmd, (
        f"'{stage.PARAM_CMD}' cannot be empty for stage '{stage.name}', "
        f"got: '{cmd}'(type: '{type(cmd).__name__}')"
    )
    res = [
        (stage.PARAM_DESC, stage.desc),
        (stage.PARAM_CMD, stage.cmd),
        (stage.PARAM_WDIR, wdir),
        (stage.PARAM_DEPS, deps),
        (stage.PARAM_PARAMS, params),
        (stage.PARAM_OUTS, outs),
        (stage.PARAM_METRICS, metrics),
        (stage.PARAM_PLOTS, plots),
        (stage.PARAM_FROZEN, stage.frozen),
        (stage.PARAM_ALWAYS_CHANGED, stage.always_changed),
        (stage.PARAM_META, stage.meta),
    ]
    return {stage.name: OrderedDict([(key, value) for key, value in res if value])}


def to_single_stage_lockfile(stage: "Stage", **kwargs) -> dict:
    from dvc.cachemgr import LEGACY_HASH_NAMES
    from dvc.dependency import DatasetDependency
    from dvc.output import (
        _serialize_hi_to_dict,
        _serialize_tree_obj_to_files,
        split_file_meta_from_cloud,
    )
    from dvc_data.hashfile.tree import Tree

    assert stage.cmd

    def _dumpd(item: "Output"):
        if isinstance(item, DatasetDependency):
            return item.dumpd()

        ret: dict[str, Any] = {item.PARAM_PATH: item.def_path}
        if item.hash_name not in LEGACY_HASH_NAMES:
            ret[item.PARAM_HASH] = "md5"
        if item.hash_info.isdir and kwargs.get("with_files"):
            obj = item.obj or item.get_obj()
            if obj:
                assert isinstance(obj, Tree)
                ret[item.PARAM_FILES] = [
                    split_file_meta_from_cloud(f)
                    for f in _serialize_tree_obj_to_files(obj)
                ]
        else:
            assert item.meta is not None
            meta_d = item.meta.to_dict()
            meta_d.pop("isdir", None)
            ret.update(_serialize_hi_to_dict(item.hash_info))
            ret.update(split_file_meta_from_cloud(meta_d))
        return ret

    res = OrderedDict([("cmd", stage.cmd)])
    params, deps = split_params_deps(stage)
    deps, outs = (
        [_dumpd(item) for item in sorted(items, key=attrgetter("def_path"))]  # type: ignore[call-overload]
        for items in [deps, stage.outs]
    )
    params = _serialize_params_values(params)
    if deps:
        res[PARAM_DEPS] = deps
    if params:
        res[PARAM_PARAMS] = params
    if outs:
        res[PARAM_OUTS] = outs

    return res


def to_lockfile(stage: "PipelineStage", **kwargs) -> dict:
    assert stage.name
    return {stage.name: to_single_stage_lockfile(stage, **kwargs)}


def to_single_stage_file(stage: "Stage", **kwargs):
    state = stage.dumpd(**kwargs)

    # When we load a stage we parse yaml with a fast parser, which strips
    # off all the comments and formatting. To retain those on update we do
    # a trick here:
    # - reparse the same yaml text with a slow but smart ruamel yaml parser
    # - apply changes to a returned structure
    # - serialize it
    text = stage._stage_text
    if text is None:
        return state

    saved_state = parse_yaml_for_update(text, stage.path)
    apply_diff(state, saved_state)
    return saved_state
