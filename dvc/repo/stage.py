import fnmatch
import logging
import os
import typing
from contextlib import suppress
from typing import Iterable, List, NamedTuple, Optional, Set, Tuple

from dvc.dvcfile import PIPELINE_FILE, Dvcfile, is_valid_filename
from dvc.exceptions import NoOutputOrStageError, OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.repo.graph import collect_inside_path, collect_pipeline
from dvc.stage.exceptions import StageFileDoesNotExistError, StageNotFound
from dvc.utils import parse_target

logger = logging.getLogger(__name__)

if typing.TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.repo import Repo
    from dvc.stage import Stage
    from dvc.stage.loader import StageLoader
    from dvc.types import OptStr


class StageInfo(NamedTuple):
    stage: "Stage"
    filter_info: Optional[PathInfo] = None


StageList = List["Stage"]
StageIter = Iterable["Stage"]
StageSet = Set["Stage"]


def _collect_with_deps(stages: StageList, graph: "DiGraph") -> StageSet:
    res: StageSet = set()
    for stage in stages:
        res.update(collect_pipeline(stage, graph=graph))
    return res


def _maybe_collect_from_dvc_yaml(
    loader: "StageLoad", target, with_deps: bool, **load_kwargs,
) -> StageIter:

    stages: StageList = []
    if loader.tree.exists(PIPELINE_FILE):
        with suppress(StageNotFound):
            stages = loader.load_all(PIPELINE_FILE, target, **load_kwargs)
    return _collect_with_deps(stages, loader.graph) if with_deps else stages


def _collect_specific_target(
    loader: "StageLoad",
    target: str,
    with_deps: bool,
    recursive: bool,
    accept_group: bool,
) -> Tuple[StageIter, "OptStr", "OptStr"]:
    # Optimization: do not collect the graph for a specific target
    file, name = parse_target(target)

    # if the target has a file, we can load directly from it.
    if not file:
        # but, if there's no file, parsing is ambiguous as it can be a
        # stage name in `dvc.yaml` file or an output. We prioritize
        # `dvc.yaml` stage name here. If it exists, then we move on.
        # else, we assume it's a output name in the `collect_granular()` below
        msg = "Checking if stage '%s' is in '%s'"
        logger.debug(msg, target, PIPELINE_FILE)
        if not (recursive and loader.tree.isdir(target)):
            stages = _maybe_collect_from_dvc_yaml(
                loader, target, with_deps, accept_group=accept_group,
            )
            if stages:
                return stages, file, name
    elif not with_deps and is_valid_filename(file):
        stages = loader.load_all(file, name, accept_group=accept_group)
        return stages, file, name
    return [], file, name


class StageLoad:
    def __init__(self, repo: "Repo") -> None:
        self.repo = repo

    def from_target(
        self, target: str, accept_group: bool = False, glob: bool = False,
    ) -> StageList:
        """
        Returns a list of stage from the provided target.
        (see load method below for further details)
        """
        path, name = parse_target(target, isa_glob=glob)
        return self.load_all(
            path=path, name=name, accept_group=accept_group, glob=glob,
        )

    def get_target(self, target: str) -> "Stage":
        """
        Returns a stage from the provided target.
        (see load_one method for further details)
        """
        path, name = parse_target(target)
        return self.load_one(path=path, name=name)

    @staticmethod
    def _get_filepath(path: str = None, name: str = None) -> str:
        if path:
            return path

        path = PIPELINE_FILE
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
        name: str = None,
        accept_group: bool = False,
        glob: bool = False,
    ) -> Iterable[str]:

        assert not (accept_group and glob)

        if not name:
            return stages.keys()

        if accept_group and stages.is_foreach_generated(name):
            return self._get_group_keys(stages, name)
        elif glob:
            return fnmatch.filter(stages.keys(), name)
        return [name]

    def load_all(
        self,
        path: str = None,
        name: str = None,
        accept_group: bool = False,
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
        from dvc.stage.loader import SingleStageLoader, StageLoader

        path = self._get_filepath(path, name)
        dvcfile = Dvcfile(self.repo, path)
        # `dvcfile.stages` is not cached
        stages = dvcfile.stages  # type: ignore

        if isinstance(stages, SingleStageLoader):
            stage = stages[name]
            return [stage]

        assert isinstance(stages, StageLoader)
        keys = self._get_keys(stages, name, accept_group, glob)
        return [stages[key] for key in keys]

    def load_one(self, path: str = None, name: str = None) -> "Stage":
        """Load a single stage from a file.

        Args:
            path: if not provided, default `dvc.yaml` is assumed.
            name: required for `dvc.yaml` files, ignored for `.dvc` files.
        """
        path = self._get_filepath(path, name)
        dvcfile = Dvcfile(self.repo, path)

        stages = dvcfile.stages  # type: ignore

        return stages[name]

    def load_file(self, path: str = None) -> StageList:
        """Load all of the stages from a file."""
        return self.load_all(path)

    def load_glob(self, path: str, expr: str = None):
        """Load stages from `path`, filtered with `expr` provided."""
        return self.load_all(path, expr, glob=True)

    @property
    def tree(self):
        return self.repo.tree

    @property
    def graph(self) -> "DiGraph":
        return self.repo.graph

    def collect(
        self,
        target: str = None,
        with_deps: bool = False,
        recursive: bool = False,
        graph: "DiGraph" = None,
        accept_group: bool = False,
        glob: bool = False,
    ) -> StageIter:
        """Collect list of stages from the provided target.

        Args:
            target: if not provided, all of the stages in the graph are
                returned.
                Target can be:
                - a stage name in the `dvc.yaml` file.
                - a path to `dvc.yaml` or `.dvc` file.
                - in case of a stage to a dvc.yaml file in a different
                  directory than current working directory, it can be a path
                  to dvc.yaml file, followed by a colon `:`, followed by stage
                  name (eg: `../dvc.yaml:build`).
                - in case of `recursive`, it can be a path to a directory.
                - in case of `accept_group`, it can be a group name of
                    `foreach` generated stage.
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
            accept_group: if true, all of the `foreach` generated stages of
                the specified target is returned.
            glob: Use `target` as a pattern to match stages in a file.
        """
        if not target:
            return list(graph) if graph else self.repo.stages

        if recursive and self.repo.tree.isdir(target):
            path = os.path.abspath(target)
            return collect_inside_path(path, graph or self.graph)

        stages = self.from_target(target, accept_group=accept_group, glob=glob)
        if not with_deps:
            return stages

        return _collect_with_deps(stages, graph or self.graph)

    def collect_granular(
        self,
        target: str = None,
        with_deps: bool = False,
        recursive: bool = False,
        graph: "DiGraph" = None,
        accept_group: bool = False,
    ) -> List[StageInfo]:
        """Collects a list of (stage, filter_info) from the given target.

        Priority is in the order of following in case of ambiguity:
        - .dvc file or .yaml file
        - dir if recursive and directory exists
        - stage_name
        - output file

        Args:
            target: if not provided, all of the stages without any filters are
                returned.
                If `target` is a path to a dvc-tracked output,
                a (stage, output_path_info) is returned.
                Otherwise, the details above for `target` in `collect()`
                applies.

            (see `collect()` for other arguments)
        """
        if not target:
            return [StageInfo(stage) for stage in self.repo.stages]

        stages, file, _ = _collect_specific_target(
            self, target, with_deps, recursive, accept_group
        )
        if not stages:
            if not (recursive and self.tree.isdir(target)):
                try:
                    (out,) = self.repo.find_outs_by_path(target, strict=False)
                    filter_info = PathInfo(os.path.abspath(target))
                    return [StageInfo(out.stage, filter_info)]
                except OutputNotFoundError:
                    pass

            try:
                stages = self.collect(
                    target,
                    with_deps,
                    recursive,
                    graph,
                    accept_group=accept_group,
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
