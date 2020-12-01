import logging
from collections.abc import Mapping
from copy import deepcopy
from itertools import chain

from funcy import (
    cached_property,
    get_in,
    lcat,
    log_durations,
    nullcontext,
    project,
)

from dvc import dependency, output
from dvc.hash_info import HashInfo
from dvc.parsing import FOREACH_KWD, JOIN, DataResolver
from dvc.path_info import PathInfo

from . import PipelineStage, Stage, loads_from
from .exceptions import StageNameUnspecified, StageNotFound
from .params import StageParams
from .utils import fill_stage_dependencies, resolve_paths

logger = logging.getLogger(__name__)


class NoopResolver:
    def __init__(self, _repo, _wdir, d):
        self.d = d
        self.tracked_vars = {}

    def resolve(self):
        return self.d


class StageLoader(Mapping):
    def __init__(self, dvcfile, data, lockfile_data=None):
        self.dvcfile = dvcfile
        self.data = data or {}
        self.stages_data = self.data.get("stages", {})
        self.repo = self.dvcfile.repo
        self._enable_parametrization = self.repo.config["feature"][
            "parametrization"
        ]
        self.lockfile_data = lockfile_data or {}

    @cached_property
    def resolver(self):
        resolver_cls = (
            DataResolver if self._enable_parametrization else NoopResolver
        )
        wdir = PathInfo(self.dvcfile.path).parent
        return resolver_cls(self.repo, wdir, self.data)

    @cached_property
    def resolved_data(self):
        data = self.data
        log = (
            log_durations(logger.debug, "resolving values")
            if self._enable_parametrization
            else nullcontext()
        )
        with log:
            data = self.resolver.resolve()
        return data.get("stages", {})

    @staticmethod
    def fill_from_lock(stage, lock_data=None):
        """Fill values for params, checksums for outs and deps from lock."""
        if not lock_data:
            return

        assert isinstance(lock_data, dict)
        items = chain(
            ((StageParams.PARAM_DEPS, dep) for dep in stage.deps),
            ((StageParams.PARAM_OUTS, out) for out in stage.outs),
        )

        checksums = {
            key: {item["path"]: item for item in lock_data.get(key, {})}
            for key in [StageParams.PARAM_DEPS, StageParams.PARAM_OUTS]
        }
        for key, item in items:
            path = item.def_path
            if isinstance(item, dependency.ParamsDependency):
                item.fill_values(get_in(lock_data, [stage.PARAM_PARAMS, path]))
                continue
            info = get_in(checksums, [key, path], {})
            info = info.copy()
            info.pop("path", None)
            item.hash_info = HashInfo.from_dict(info)

    @classmethod
    def load_stage(cls, dvcfile, name, stage_data, lock_data=None):
        assert all([name, dvcfile, dvcfile.repo, dvcfile.path])
        assert stage_data and isinstance(stage_data, dict)

        path, wdir = resolve_paths(
            dvcfile.path, stage_data.get(Stage.PARAM_WDIR)
        )
        stage = loads_from(PipelineStage, dvcfile.repo, path, wdir, stage_data)
        stage.name = name
        stage.desc = stage_data.get(Stage.PARAM_DESC)
        stage.meta = stage_data.get(Stage.PARAM_META)

        deps = project(stage_data, [stage.PARAM_DEPS, stage.PARAM_PARAMS])
        fill_stage_dependencies(stage, **deps)

        outs = project(
            stage_data,
            [stage.PARAM_OUTS, stage.PARAM_METRICS, stage.PARAM_PLOTS],
        )
        stage.outs = lcat(
            output.load_from_pipeline(stage, data, typ=key)
            for key, data in outs.items()
        )

        if lock_data:
            stage.cmd_changed = lock_data.get(Stage.PARAM_CMD) != stage.cmd

        cls.fill_from_lock(stage, lock_data)
        return stage

    def __getitem__(self, name):
        if not name:
            raise StageNameUnspecified(self.dvcfile)

        if name not in self:
            raise StageNotFound(self.dvcfile, name)

        if not self.lockfile_data.get(name):
            logger.debug(
                "No lock entry found for '%s:%s'", self.dvcfile.relpath, name,
            )

        stage = self.load_stage(
            self.dvcfile,
            name,
            self.resolved_data[name],
            self.lockfile_data.get(name, {}),
        )

        stage.tracked_vars = self.resolver.tracked_vars.get(name, {})
        group, *keys = name.rsplit(JOIN, maxsplit=1)
        if group and keys and name not in self.stages_data:
            stage.raw_data.generated_from = group

        stage.raw_data.parametrized = (
            self.stages_data.get(name, {}) != self.resolved_data[name]
        )

        return stage

    def __iter__(self):
        return iter(self.resolved_data)

    def __len__(self):
        return len(self.resolved_data)

    def __contains__(self, name):
        return name in self.resolved_data

    def is_foreach_generated(self, name: str):
        return (
            name in self.stages_data and FOREACH_KWD in self.stages_data[name]
        )


class SingleStageLoader(Mapping):
    def __init__(self, dvcfile, stage_data, stage_text=None):
        self.dvcfile = dvcfile
        self.stage_data = stage_data or {}
        self.stage_text = stage_text

    def __getitem__(self, item):
        if item:
            logger.warning(
                "Ignoring name '%s' for single stage in '%s'.",
                item,
                self.dvcfile,
            )
        # during `load`, we remove attributes from stage data, so as to
        # not duplicate, therefore, for MappingView, we need to deepcopy.
        return self.load_stage(
            self.dvcfile, deepcopy(self.stage_data), self.stage_text
        )

    @classmethod
    def load_stage(cls, dvcfile, d, stage_text):
        path, wdir = resolve_paths(dvcfile.path, d.get(Stage.PARAM_WDIR))
        stage = loads_from(Stage, dvcfile.repo, path, wdir, d)
        stage._stage_text = stage_text  # noqa, pylint:disable=protected-access
        stage.deps = dependency.loadd_from(
            stage, d.get(Stage.PARAM_DEPS) or []
        )
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS) or [])
        return stage

    def __iter__(self):
        return iter([None])

    def __contains__(self, item):
        return False

    def __len__(self):
        return 1
