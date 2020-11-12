import logging
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from copy import deepcopy
from itertools import starmap
from typing import TYPE_CHECKING

from funcy import first, join

from dvc.dependency.param import ParamsDependency
from dvc.path_info import PathInfo

from .context import Context

if TYPE_CHECKING:
    from dvc.repo import Repo

logger = logging.getLogger(__name__)

STAGES_KWD = "stages"
USE_KWD = "use"
VARS_KWD = "vars"
WDIR_KWD = "wdir"
DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE
PARAMS_KWD = "params"
FOREACH_KWD = "foreach"
IN_KWD = "in"
SET_KWD = "set"

DEFAULT_SENTINEL = object()


class DataResolver:
    def __init__(self, repo: "Repo", wdir: PathInfo, d: dict):
        to_import: PathInfo = wdir / d.get(USE_KWD, DEFAULT_PARAMS_FILE)
        vars_ = d.get(VARS_KWD, {})
        vars_ctx = Context(vars_)
        if os.path.exists(to_import):
            self.global_ctx_source = to_import
            self.global_ctx = Context.load_from(repo.tree, str(to_import))
        else:
            self.global_ctx = Context()
            self.global_ctx_source = None
            logger.debug(
                "%s does not exist, it won't be used in parametrization",
                to_import,
            )

        self.global_ctx.merge_update(vars_ctx)
        self.data: dict = d
        self.wdir = wdir
        self.repo = repo

    def _resolve_entry(self, name: str, definition):
        context = Context.clone(self.global_ctx)
        if FOREACH_KWD in definition:
            self.set_context_from(context, definition.get(SET_KWD, {}))
            assert IN_KWD in definition
            return self._foreach(
                context, name, definition[FOREACH_KWD], definition[IN_KWD]
            )
        return self._resolve_stage(context, name, definition)

    def resolve(self):
        stages = self.data.get(STAGES_KWD, {})
        data = join(starmap(self._resolve_entry, stages.items()))
        logger.trace("Resolved dvc.yaml:\n%s", data)
        return {STAGES_KWD: data}

    def _resolve_stage(self, context: Context, name: str, definition) -> dict:
        definition = deepcopy(definition)
        self.set_context_from(context, definition.pop(SET_KWD, {}))
        wdir = self._resolve_wdir(context, definition.get(WDIR_KWD))
        if self.wdir != wdir:
            logger.debug(
                "Stage %s has different wdir than dvc.yaml file", name
            )

        contexts = []
        params_yaml_file = wdir / DEFAULT_PARAMS_FILE
        if self.global_ctx_source != params_yaml_file:
            if os.path.exists(params_yaml_file):
                contexts.append(
                    Context.load_from(self.repo.tree, str(params_yaml_file))
                )
            else:
                logger.debug(
                    "%s does not exist for stage %s", params_yaml_file, name
                )

        params_deps = definition.get(PARAMS_KWD, [])
        params_files = {
            wdir / first(item)
            for item in params_deps
            if item and isinstance(item, dict)
        }
        for params_file in params_files - {
            self.global_ctx_source,
            params_yaml_file,
        }:
            contexts.append(
                Context.load_from(self.repo.tree, str(params_file))
            )

        context.merge_update(*contexts)

        logger.trace(  # pytype: disable=attribute-error
            "Context during resolution of stage %s:\n%s", name, context
        )

        with context.track():
            stage_d = context.resolve(definition)

        params = stage_d.get(PARAMS_KWD, []) + self._resolve_params(
            context, wdir
        )

        if params:
            stage_d[PARAMS_KWD] = params
        return {name: stage_d}

    def _resolve_params(self, context: Context, wdir):
        tracked = defaultdict(set)
        for src, keys in context.tracked.items():
            tracked[str(PathInfo(src).relative_to(wdir))].update(keys)

        return [{file: list(keys)} for file, keys in tracked.items()]

    def _resolve_wdir(self, context: Context, wdir: str = None) -> PathInfo:
        if not wdir:
            return self.wdir

        wdir = str(context.resolve_str(wdir, unwrap=True))
        return self.wdir / str(wdir)

    def _foreach(self, context: Context, name: str, foreach_data, in_data):
        def each_iter(value, key=DEFAULT_SENTINEL):
            c = Context.clone(context)
            c["item"] = value
            if key is not DEFAULT_SENTINEL:
                c["key"] = key
            suffix = str(key if key is not DEFAULT_SENTINEL else value)
            return self._resolve_stage(c, f"{name}-{suffix}", in_data)

        iterable = context.resolve(foreach_data, unwrap=False)

        assert isinstance(iterable, (Sequence, Mapping)) and not isinstance(
            iterable, str
        ), f"got type of {type(iterable)}"
        if isinstance(iterable, Sequence):
            gen = (each_iter(v) for v in iterable)
        else:
            gen = (each_iter(v, k) for k, v in iterable.items())
        return join(gen)

    @classmethod
    def set_context_from(cls, context: Context, to_set):
        for key, value in to_set.items():
            context.set(key, value)
