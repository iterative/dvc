import logging
from collections.abc import Mapping
from copy import deepcopy
from itertools import chain
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple

from funcy import get_in, lcat, once, project

from dvc import dependency, output
from dvc.parsing import FOREACH_KWD, JOIN, MATRIX_KWD, EntryNotFound
from dvc.utils.objects import cached_property
from dvc_data.hashfile.meta import Meta

from . import PipelineStage, Stage, loads_from
from .exceptions import StageNameUnspecified, StageNotFound
from .params import StageParams
from .utils import fill_stage_dependencies, resolve_paths

if TYPE_CHECKING:
    from dvc.dvcfile import ProjectFile, SingleStageFile

logger = logging.getLogger(__name__)


class StageLoader(Mapping):
    def __init__(
        self,
        dvcfile: "ProjectFile",
        data,
        lockfile_data=None,
    ):
        self.dvcfile = dvcfile
        self.resolver = self.dvcfile.resolver
        self.data = data or {}
        self.stages_data = self.data.get("stages", {})
        self.repo = self.dvcfile.repo

        lockfile_data = lockfile_data or {}
        self._lockfile_data = lockfile_data.get("stages", {})

    @cached_property
    def lockfile_data(self) -> Dict[str, Any]:
        if not self._lockfile_data:
            logger.debug("Lockfile for '%s' not found", self.dvcfile.relpath)
        return self._lockfile_data

    @staticmethod
    def fill_from_lock(stage, lock_data=None):
        """Fill values for params, checksums for outs and deps from lock."""
        if not lock_data:
            return

        from dvc.output import Output, merge_file_meta_from_cloud

        assert isinstance(lock_data, dict)
        items: Iterable[Tuple[str, "Output"]] = chain(
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

            hash_name = info.pop(Output.PARAM_HASH, None)
            item.meta = Meta.from_dict(merge_file_meta_from_cloud(info))
            # pylint: disable-next=protected-access
            item.hash_name, item.hash_info = item._compute_hash_info_from_meta(
                hash_name
            )
            files = get_in(checksums, [key, path, item.PARAM_FILES], None)
            if files:
                item.files = [merge_file_meta_from_cloud(f) for f in files]
            # pylint: disable-next=protected-access
            item._compute_meta_hash_info_from_files()

    @classmethod
    def load_stage(cls, dvcfile: "ProjectFile", name, stage_data, lock_data=None):
        assert all([name, dvcfile, dvcfile.repo, dvcfile.path])
        assert stage_data
        assert isinstance(stage_data, dict)

        path, wdir = resolve_paths(
            dvcfile.repo.fs, dvcfile.path, stage_data.get(Stage.PARAM_WDIR)
        )
        stage = loads_from(PipelineStage, dvcfile.repo, path, wdir, stage_data)
        stage.name = name
        stage.desc = stage_data.get(Stage.PARAM_DESC)
        stage.meta = stage_data.get(Stage.PARAM_META)

        deps = project(stage_data, [stage.PARAM_DEPS, stage.PARAM_PARAMS])
        fill_stage_dependencies(stage, **deps)

        outs = project(
            stage_data,
            [
                stage.PARAM_OUTS,
                stage.PARAM_METRICS,
                stage.PARAM_PLOTS,
            ],
        )
        stage.outs = lcat(
            output.load_from_pipeline(stage, data, typ=key)
            for key, data in outs.items()
        )

        if lock_data:
            stage.cmd_changed = lock_data.get(Stage.PARAM_CMD) != stage.cmd

        cls.fill_from_lock(stage, lock_data)
        return stage

    @once
    def lockfile_needs_update(self):
        # if lockfile does not have all of the entries that dvc.yaml says it
        # should have, provide a debug message once
        # pylint: disable=protected-access
        lockfile = self.dvcfile._lockfile.relpath
        logger.debug("Lockfile '%s' needs to be updated.", lockfile)

    def __getitem__(self, name):
        if not name:
            raise StageNameUnspecified(self.dvcfile)

        try:
            resolved_data = self.resolver.resolve_one(name)
        except EntryNotFound:
            raise StageNotFound(self.dvcfile, name)  # noqa: B904

        if self.lockfile_data and name not in self.lockfile_data:
            self.lockfile_needs_update()
            logger.trace(  # type: ignore[attr-defined]
                "No lock entry found for '%s:%s'", self.dvcfile.relpath, name
            )

        resolved_stage = resolved_data[name]
        stage = self.load_stage(
            self.dvcfile,
            name,
            resolved_stage,
            self.lockfile_data.get(name, {}),
        )

        stage.tracked_vars = self.resolver.tracked_vars.get(name, {})
        group, *keys = name.rsplit(JOIN, maxsplit=1)
        if group and keys and name not in self.stages_data:
            stage.raw_data.generated_from = group

        stage.raw_data.parametrized = self.stages_data.get(name, {}) != resolved_stage
        return stage

    def __iter__(self):
        return iter(self.resolver.get_keys())

    def __len__(self):
        return len(self.resolver.get_keys())

    def __contains__(self, name):
        return self.resolver.has_key(name)  # noqa: W601

    def is_foreach_or_matrix_generated(self, name: str) -> bool:
        return (
            name in self.stages_data
            and {FOREACH_KWD, MATRIX_KWD} & self.stages_data[name].keys()
        )


class SingleStageLoader(Mapping):
    def __init__(
        self,
        dvcfile: "SingleStageFile",
        stage_data: Dict[Any, str],
        stage_text: Optional[str] = None,
    ):
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
        return self.load_stage(self.dvcfile, deepcopy(self.stage_data), self.stage_text)

    @classmethod
    def load_stage(
        cls,
        dvcfile: "SingleStageFile",
        d: Dict[str, Any],
        stage_text: Optional[str],
    ) -> Stage:
        path, wdir = resolve_paths(
            dvcfile.repo.fs, dvcfile.path, d.get(Stage.PARAM_WDIR)
        )
        stage = loads_from(Stage, dvcfile.repo, path, wdir, d)
        stage._stage_text = stage_text  # pylint: disable=protected-access
        stage.deps = dependency.loadd_from(stage, d.get(Stage.PARAM_DEPS) or [])
        stage.outs = output.loadd_from(stage, d.get(Stage.PARAM_OUTS) or [])
        return stage

    def __iter__(self):
        return iter([None])

    def __contains__(self, item):
        return False

    def __len__(self):
        return 1
