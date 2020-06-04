import logging
import os
from collections.abc import Mapping
from copy import deepcopy
from itertools import chain

from funcy import lcat, project

from dvc import dependency, output

from ..dependency import ParamsDependency
from . import fill_stage_dependencies
from .exceptions import StageNameUnspecified, StageNotFound

logger = logging.getLogger(__name__)

DEFAULT_PARAMS_FILE = ParamsDependency.DEFAULT_PARAMS_FILE


def resolve_paths(path, wdir=None):
    path = os.path.abspath(path)
    wdir = wdir or os.curdir
    wdir = os.path.abspath(os.path.join(os.path.dirname(path), wdir))
    return path, wdir


class StageLoader(Mapping):
    def __init__(self, dvcfile, stages_data, lockfile_data=None):
        self.dvcfile = dvcfile
        self.stages_data = stages_data or {}
        self.lockfile_data = lockfile_data or {}

    @staticmethod
    def fill_from_lock(stage, lock_data):
        """Fill values for params, checksums for outs and deps from lock."""
        from .params import StageParams

        items = chain(
            ((StageParams.PARAM_DEPS, dep) for dep in stage.deps),
            ((StageParams.PARAM_OUTS, out) for out in stage.outs),
        )

        checksums = {
            key: {item["path"]: item for item in lock_data.get(key, {})}
            for key in [StageParams.PARAM_DEPS, StageParams.PARAM_OUTS]
        }
        for key, item in items:
            if isinstance(item, ParamsDependency):
                # load the params with values inside lock dynamically
                lock_params = lock_data.get(stage.PARAM_PARAMS, {})
                item.fill_values(lock_params.get(item.def_path, {}))
                continue

            item.checksum = (
                checksums.get(key, {})
                .get(item.def_path, {})
                .get(item.checksum_type)
            )

    @classmethod
    def load_stage(cls, dvcfile, name, stage_data, lock_data):
        from . import PipelineStage, Stage, loads_from

        path, wdir = resolve_paths(
            dvcfile.path, stage_data.get(Stage.PARAM_WDIR)
        )
        stage = loads_from(PipelineStage, dvcfile.repo, path, wdir, stage_data)
        stage.name = name

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
            stage.cmd_changed = lock_data.get(
                Stage.PARAM_CMD
            ) != stage_data.get(Stage.PARAM_CMD)
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

        return self.load_stage(
            self.dvcfile,
            name,
            self.stages_data[name],
            self.lockfile_data.get(name, {}),
        )

    def __iter__(self):
        return iter(self.stages_data)

    def __len__(self):
        return len(self.stages_data)

    def __contains__(self, name):
        return name in self.stages_data


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
        from dvc.stage import Stage, loads_from

        path, wdir = resolve_paths(dvcfile.path, d.get(Stage.PARAM_WDIR))
        stage = loads_from(Stage, dvcfile.repo, path, wdir, d)
        stage._stage_text = stage_text
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
