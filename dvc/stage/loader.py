import logging
import os
from contextlib import contextmanager

from copy import deepcopy
from collections import defaultdict, Mapping
from itertools import chain

from funcy import first

from dvc import dependency, output
from .exceptions import StageNameUnspecified, StageNotFound
from ..dependency import ParamsDependency

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
        self._log_level = logging.WARNING

    @contextmanager
    def log_level(self, at):
        """Change log_level temporarily for StageLoader."""
        self._log_level, level = at, self._log_level
        yield
        self._log_level = level

    def filter(self, item=None):
        if not item:
            return self

        if item not in self:
            raise StageNotFound(self.dvcfile, item)

        data = {item: self.stages_data[item]} if item in self else {}
        return self.__class__(self.dvcfile, data, self.lockfile_data)

    @staticmethod
    def fill_from_lock(stage, lock_data):
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
                params = lock_data.get("params", {}).get(item.def_path, {})
                item._dyn_load(params)
                continue

            item.checksum = (
                checksums.get(key, {})
                .get(item.def_path, {})
                .get(item.checksum_type)
            )

    @classmethod
    def _load_params(cls, stage, pipeline_params):
        """
        File in pipeline file is expected to be in following format:
        ```
        params:
            - lr
            - train.epochs
            - params2.yaml:  # notice the filename
                - process.threshold
                - process.bow
        ```

        and, in lockfile, we keep it as following format:
        ```
        params:
          params.yaml:
            lr: 0.0041
            train.epochs: 100
          params2.yaml:
            process.threshold: 0.98
            process.bow:
            - 15000
            - 123
        ```
        In the list of `params` inside pipeline file, if any of the item is
        dict-like, the key will be treated as separate params file and it's
        values to be part of that params file, else, the item is considered
        as part of the `params.yaml` which is a default file.

        (From example above: `lr` is considered to be part of `params.yaml`
        whereas `process.bow` to be part of `params2.yaml`.)

        We only load the keys here, lockfile bears the values which are used
        to compare between the actual params from the file in the workspace.
        """
        res = defaultdict(list)
        for key in pipeline_params:
            if isinstance(key, str):
                path = DEFAULT_PARAMS_FILE
                res[path].append(key)
            elif isinstance(key, dict):
                path = first(key)
                res[path].extend(key[path])

        stage.deps += dependency.loadd_from(
            stage,
            [{"path": key, "params": params} for key, params in res.items()],
        )

    @classmethod
    def load_stage(cls, dvcfile, name, stage_data, lock_data):
        from . import PipelineStage, Stage, loads_from

        path, wdir = resolve_paths(
            dvcfile.path, stage_data.get(Stage.PARAM_WDIR)
        )
        stage = loads_from(PipelineStage, dvcfile.repo, path, wdir, stage_data)
        stage.name = name
        params = stage_data.pop("params", {})
        stage._fill_stage_dependencies(**stage_data)
        stage._fill_stage_outputs(**stage_data)
        cls._load_params(stage, params)
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
            logger.log(
                self._log_level,
                "No lock entry found for '%s:%s'",
                self.dvcfile.relpath,
                name,
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

    def filter(self, item=None):
        return self

    @contextmanager
    def log_level(self, *args, **kwargs):
        """No-op context manager."""
        yield

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
