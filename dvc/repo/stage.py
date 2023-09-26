import fnmatch
import logging
import typing
from contextlib import suppress
from functools import wraps
from typing import Iterable, List, NamedTuple, Optional, Set, Tuple, Union

from dvc.exceptions import (
    NoOutputOrStageError,
    OutputDuplicationError,
    OutputNotFoundError,
)
from dvc.repo import lock_repo
from dvc.ui import ui
from dvc.utils import as_posix, parse_target

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.repo import Repo
    from dvc.stage import PipelineStage, Stage
    from dvc.stage.loader import StageLoader

PROJECT_FILE = "dvc.yaml"


class StageInfo(NamedTuple):
    stage: "Stage"
    filter_info: Optional[str] = None


StageList = List["Stage"]
StageIter = Iterable["Stage"]
StageSet = Set["Stage"]


def _collect_with_deps(stages: StageList, graph: "DiGraph") -> StageSet:
    from dvc.exceptions import StageNotFoundError
    from dvc.repo.graph import collect_pipeline

    res: StageSet = set()
    for stage in stages:
        pl = list(collect_pipeline(stage, graph=graph))
        if not pl:
            raise StageNotFoundError(
                f"Stage {stage} is not found in the project. "
                "Check that there are no symlinks in the parents "
                "leading up to it within the project."
            )
        res.update(pl)
    return res


def _maybe_collect_from_dvc_yaml(
    loader: "StageLoad", target, with_deps: bool, **load_kwargs
) -> StageIter:
    from dvc.stage.exceptions import StageNotFound

    stages: StageList = []
    if loader.fs.exists(PROJECT_FILE):
        with suppress(StageNotFound):
            stages = loader.load_all(PROJECT_FILE, target, **load_kwargs)
    if with_deps:
        return _collect_with_deps(stages, loader.repo.index.graph)
    return stages


def _collect_specific_target(
    loader: "StageLoad",
    target: str,
    with_deps: bool,
    recursive: bool,
) -> Tuple[StageIter, Optional[str], Optional[str]]:
    from dvc.dvcfile import is_valid_filename

    # Optimization: do not collect the graph for a specific target
    file, name = parse_target(target)

    # if the target has a file, we can load directly from it.
    if not file:
        # but, if there's no file, parsing is ambiguous as it can be a
        # stage name in `dvc.yaml` file or an output. We prioritize
        # `dvc.yaml` stage name here. If it exists, then we move on.
        # else, we assume it's a output name in the `collect_granular()` below
        msg = "Checking if stage '%s' is in '%s'"
        logger.debug(msg, target, PROJECT_FILE)
        if not (recursive and loader.fs.isdir(target)):
            stages = _maybe_collect_from_dvc_yaml(loader, target, with_deps)
            if stages:
                return stages, file, name
    elif not with_deps and is_valid_filename(file):
        stages = loader.load_all(file, name)
        return stages, file, name
    return [], file, name


def locked(f):
    @wraps(f)
    def wrapper(loader: "StageLoad", *args, **kwargs):
        with lock_repo(loader.repo):
            return f(loader, *args, **kwargs)

    return wrapper


class StageLoad:
    def __init__(self, repo: "Repo") -> None:
        self.repo: "Repo" = repo

    @property
    def fs(self):
        return self.repo.fs

    @locked
    def add(
        self,
        single_stage: bool = False,
        fname: Optional[str] = None,
        validate: bool = True,
        force: bool = False,
        update_lock: bool = False,
        **stage_data,
    ):
        stage = self.create(
            single_stage=single_stage,
            fname=fname,
            validate=validate,
            force=force,
            **stage_data,
        )
        stage.dump(update_lock=update_lock)
        try:
            stage.ignore_outs()
        except FileNotFoundError as exc:
            ui.warn(
                f"Could not create .gitignore entry in {exc.filename}."
                " DVC will attempt to create .gitignore entry again when"
                " the stage is run."
            )

        return stage

    def create(
        self,
        single_stage: bool = False,
        validate: bool = True,
        fname: Optional[str] = None,
        force: bool = False,
        **stage_data,
    ) -> Union["Stage", "PipelineStage"]:
        """Creates a stage.

        Args:
            single_stage: if true, the .dvc file based stage is created,
                fname is required in that case
            fname: name of the file to use, not used for dvc.yaml files
            validate: if true, the new created stage is checked against the
                stages in the repo. Eg: graph correctness,
                potential overwrites in dvc.yaml file (unless `force=True`).
            force: ignores overwrites in dvc.yaml file
            stage_data: Stage data to create from
                (see create_stage and loads_from for more information)
        """
        from dvc.stage import PipelineStage, Stage, create_stage, restore_fields
        from dvc.stage.exceptions import InvalidStageName
        from dvc.stage.utils import is_valid_name, prepare_file_path, validate_kwargs

        stage_data = validate_kwargs(
            single_stage=single_stage, fname=fname, **stage_data
        )
        if single_stage:
            stage_cls = Stage
            path = fname or prepare_file_path(stage_data)
        else:
            path = PROJECT_FILE
            stage_cls = PipelineStage
            stage_name = stage_data["name"]
            if not (stage_name and is_valid_name(stage_name)):
                raise InvalidStageName

        stage = create_stage(stage_cls, repo=self.repo, path=path, **stage_data)
        if validate:
            if not force:
                from dvc.stage.utils import check_stage_exists

                check_stage_exists(self.repo, stage, stage.path)

            try:
                self.repo.check_graph(stages={stage})
            except OutputDuplicationError as exc:
                # Don't include the stage currently being added.
                exc.stages.remove(stage)
                raise OutputDuplicationError(exc.output, exc.stages) from None

        restore_fields(stage)
        return stage

    def from_target(
        self, target: str, accept_group: bool = True, glob: bool = False
    ) -> StageList:
        """
        Returns a list of stage from the provided target.
        (see load method below for further details)
        """
        path, name = parse_target(target, isa_glob=glob)
        return self.load_all(path=path, name=name, accept_group=accept_group, glob=glob)

    def get_target(self, target: str) -> "Stage":
        """
        Returns a stage from the provided target.
        (see load_one method for further details)
        """
        path, name = parse_target(target)
        return self.load_one(path=path, name=name)

    def _get_filepath(
        self, path: Optional[str] = None, name: Optional[str] = None
    ) -> str:
        if path:
            return self.repo.fs.path.abspath(path)

        path = PROJECT_FILE
        logger.debug("Assuming '%s' to be a stage inside '%s'", name, path)
        return path

    @staticmethod
    def _get_group_keys(stages: "StageLoader", group: str) -> Iterable[str]:
        from dvc.parsing import JOIN

        for key in stages:
            assert isinstance(key, str)
            if key.startswith(f"{group}{JOIN}"):
                yield key

    def _get_keys(
        self,
        stages: "StageLoader",
        name: Optional[str] = None,
        accept_group: bool = True,
        glob: bool = False,
    ) -> Iterable[str]:
        if not name:
            return stages.keys()
        if accept_group and stages.is_foreach_or_matrix_generated(name):
            return self._get_group_keys(stages, name)
        if glob:
            return fnmatch.filter(stages.keys(), name)
        return [name]

    def load_all(
        self,
        path: Optional[str] = None,
        name: Optional[str] = None,
        accept_group: bool = True,
        glob: bool = False,
    ) -> StageList:
        """Load a list of stages from a file.

        Args:
            path: if not provided, default `dvc.yaml` is assumed.
            name: required for `dvc.yaml` files, ignored for `.dvc` files.
            accept_group: if true, all of the the stages generated from `name`
                foreach are returned.
            glob: if true, `name` is considered as a glob, which is
                used to filter list of stages from the given `path`.
        """
        from dvc.dvcfile import load_file
        from dvc.stage.loader import SingleStageLoader, StageLoader

        path = self._get_filepath(path, name)
        dvcfile = load_file(self.repo, path)
        # `dvcfile.stages` is not cached
        stages = dvcfile.stages  # type: ignore[attr-defined]

        if isinstance(stages, SingleStageLoader):
            stage = stages[name]
            return [stage]

        assert isinstance(stages, StageLoader)
        keys = self._get_keys(stages, name, accept_group, glob)
        return [stages[key] for key in keys]

    def load_one(
        self, path: Optional[str] = None, name: Optional[str] = None
    ) -> "Stage":
        """Load a single stage from a file.

        Args:
            path: if not provided, default `dvc.yaml` is assumed.
            name: required for `dvc.yaml` files, ignored for `.dvc` files.
        """
        from dvc.dvcfile import load_file

        path = self._get_filepath(path, name)
        dvcfile = load_file(self.repo, path)
        stages = dvcfile.stages  # type: ignore[attr-defined]

        return stages[name]

    def load_file(self, path: Optional[str] = None) -> StageList:
        """Load all of the stages from a file."""
        return self.load_all(path)

    def load_glob(self, path: str, expr: Optional[str] = None):
        """Load stages from `path`, filtered with `expr` provided."""
        return self.load_all(path, expr, glob=True)

    def collect(
        self,
        target: Optional[str] = None,
        with_deps: bool = False,
        recursive: bool = False,
        graph: Optional["DiGraph"] = None,
        glob: bool = False,
    ) -> StageIter:
        """Collect list of stages from the provided target.

        Args:
            target: if not provided, all of the stages in the graph are
                returned.
                Target can be:
                - a foreach group name or a stage name in the `dvc.yaml` file.
                - a generated stage name from a foreach group.
                - a path to `dvc.yaml` or `.dvc` file.
                - in case of a stage to a dvc.yaml file in a different
                  directory than current working directory, it can be a path
                  to dvc.yaml file, followed by a colon `:`, followed by stage
                  name (eg: `../dvc.yaml:build`).
                - in case of `recursive`, it can be a path to a directory.
                - in case of `glob`, it can be a wildcard pattern to match
                  stages. Example: `build*` for stages in `dvc.yaml` file, or
                  `../dvc.yaml:build*` for stages in dvc.yaml in a different
                  directory.
                  Note that, glob only applies for the stage name, not to the
                  file, so `**/dvc.yaml:build*` is not possible.
            with_deps: if true, the stages including their dependencies are
                returned.
            recursive: if true and if `target` is a directory, all of the
                stages inside that directory is returned.
            graph: graph to use. Defaults to `repo.graph`.
            glob: Use `target` as a pattern to match stages in a file.
        """
        if not target:
            return list(graph) if graph else self.repo.index.stages

        if recursive and self.fs.isdir(target):
            from dvc.repo.graph import collect_inside_path

            path = self.fs.path.abspath(target)
            return collect_inside_path(path, graph or self.repo.index.graph)

        stages = self.from_target(target, glob=glob)
        if not with_deps:
            return stages

        return _collect_with_deps(stages, graph or self.repo.index.graph)

    def collect_granular(
        self,
        target: Optional[str] = None,
        with_deps: bool = False,
        recursive: bool = False,
        graph: Optional["DiGraph"] = None,
    ) -> List[StageInfo]:
        """Collects a list of (stage, filter_info) from the given target.

        Priority is in the order of following in case of ambiguity:
        - .dvc file or .yaml file
        - dir if recursive and directory exists
        - foreach_group_name or stage_name
        - generated stage name from a foreach group
        - output file

        Args:
            target: if not provided, all of the stages without any filters are
                returned.
                If `target` is a path to a dvc-tracked output,
                a (stage, output_path) is returned.
                Otherwise, the details above for `target` in `collect()`
                applies.

            (see `collect()` for other arguments)
        """
        if not target:
            return [StageInfo(stage) for stage in self.repo.index.stages]

        target = as_posix(target)

        stages, file, _ = _collect_specific_target(self, target, with_deps, recursive)
        if not stages:
            if not (recursive and self.fs.isdir(target)):
                try:
                    (out,) = self.repo.find_outs_by_path(target, strict=False)
                    return [StageInfo(out.stage, self.fs.path.abspath(target))]
                except OutputNotFoundError:
                    pass

            from dvc.dvcfile import is_valid_filename
            from dvc.stage.exceptions import StageFileDoesNotExistError, StageNotFound

            try:
                stages = self.collect(
                    target,
                    with_deps,
                    recursive,
                    graph,
                )
            except StageFileDoesNotExistError as exc:
                # collect() might try to use `target` as a stage name
                # and throw error that dvc.yaml does not exist, whereas it
                # should say that both stage name and file does not exist.
                if file and is_valid_filename(file):
                    raise
                raise NoOutputOrStageError(target, exc.file) from exc
            except StageNotFound as exc:
                raise NoOutputOrStageError(target, exc.file) from exc

        return [StageInfo(stage) for stage in stages]
