from contextlib import suppress
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
)

from funcy import cached_property, nullcontext

from dvc.utils import dict_md5

if TYPE_CHECKING:
    from networkx import DiGraph
    from pygtrie import Trie

    from dvc.dependency import Dependency, ParamsDependency
    from dvc.fs import FileSystem
    from dvc.output import Output
    from dvc.repo.stage import StageInfo, StageLoad
    from dvc.stage import Stage
    from dvc.types import StrPath, TargetType
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.index import DataIndex, DataIndexKey, DataIndexView
    from dvc_objects.db import ObjectDB


ObjectContainer = Dict[Optional["ObjectDB"], Set["HashInfo"]]


class Index:
    def __init__(
        self,
        repo: "Repo",  # pylint: disable=redefined-outer-name
        fs: "FileSystem" = None,
        stages: List["Stage"] = None,
    ) -> None:
        """Index is an immutable collection of stages.

        Generally, Index is a complete collection of stages at a point in time.
        With "a point in time", it means it is collected from the user's
        workspace or a git revision.
        And, since Index is immutable, the collection is frozen in time.

        Index provides multiple ways to view this collection:

            stages - provides direct access to this collection
            outputs - provides direct access to the outputs
            objects - provides direct access to the objects
            graph -
            ... and many more.

        Index also provides ways to slice and dice this collection.
        Some `views` might not make sense when sliced (eg: pipelines/graph).
        """

        self.repo: "Repo" = repo
        self.fs: "FileSystem" = fs or repo.fs
        self.stage_collector: "StageLoad" = repo.stage
        if stages is not None:
            self.stages: List["Stage"] = stages
        self._collected_targets: Dict[int, List["StageInfo"]] = {}

    @cached_property
    def stages(self) -> List["Stage"]:  # pylint: disable=method-hidden
        # note that ideally we should be keeping this in a set as it is unique,
        # hashable and has no concept of orderliness on its own. But we depend
        # on this to be somewhat ordered for status/metrics/plots, etc.
        onerror = self.repo.stage_collection_error_handler
        return self.stage_collector.collect_repo(onerror=onerror)

    def __repr__(self) -> str:
        from dvc.fs import LocalFileSystem

        rev = "workspace"
        if not isinstance(self.fs, LocalFileSystem):
            rev = self.repo.get_rev()[:7]
        return f"Index({self.repo}, fs@{rev})"

    def __len__(self) -> int:
        return len(self.stages)

    def __contains__(self, stage: "Stage") -> bool:
        # as we are keeping stages inside a list, it might be slower.
        return stage in self.stages

    def __iter__(self) -> Iterator["Stage"]:
        yield from self.stages

    def __getitem__(self, item: str) -> "Stage":
        """Get a stage by its addressing attribute."""
        for stage in self:
            if stage.addressing == item:
                return stage
        raise KeyError(f"{item} - available stages are {self.stages}")

    def filter(self, filter_fn: Callable[["Stage"], bool]) -> "Index":
        stages_it = filter(filter_fn, self)
        return Index(self.repo, self.fs, stages=list(stages_it))

    def slice(self, path: "StrPath") -> "Index":
        from dvc.utils import relpath
        from dvc.utils.fs import path_isin

        target_path = relpath(path, self.repo.root_dir)

        def is_stage_inside_path(stage: "Stage") -> bool:
            return path_isin(stage.path_in_repo, target_path)

        return self.filter(is_stage_inside_path)

    @staticmethod
    def _hash_targets(
        targets: Iterable[Optional[str]],
        **kwargs: Any,
    ) -> int:
        return hash(
            (
                frozenset(targets),
                kwargs.get("with_deps", False),
                kwargs.get("recursive", False),
            )
        )

    def _collect_targets(
        self, targets: Optional["TargetType"], **kwargs: Any
    ) -> List["StageInfo"]:
        from itertools import chain

        from dvc.repo.stage import StageInfo
        from dvc.utils.collections import ensure_list

        targets = ensure_list(targets)
        if not targets:
            return [StageInfo(stage) for stage in self.stages]
        targets_hash = self._hash_targets(targets, **kwargs)
        if targets_hash not in self._collected_targets:
            self._collected_targets[targets_hash] = list(
                chain.from_iterable(
                    self.stage_collector.collect_granular(target, **kwargs)
                    for target in targets
                )
            )
        return self._collected_targets[targets_hash]

    def targets_view(
        self,
        targets: "TargetType",
        stage_filter: Optional[Callable[["Stage"], bool]] = None,
        outs_filter: Optional[Callable[["Output"], bool]] = None,
        **kwargs: Any,
    ) -> "IndexView":
        """Return read-only view of index for the specified targets.

        Args:
            targets: Targets to collect
            stage_filter: Optional stage filter to be applied after collecting
                targets.
            outs_filter: Optional output filter to be applied after collecting
                targets.

        Additional kwargs will be passed into the stage collector.

        Note:
            If both stage_filter and outs_filter are provided, stage_filter
            will be applied first, and the resulting view will only contain
            outputs from stages that matched stage_filter. Outputs from stages
            that did not match will be excluded from the view (whether or not
            the output would have matched outs_filter).
        """
        stage_infos = [
            stage_info
            for stage_info in self._collect_targets(targets, **kwargs)
            if not stage_filter or stage_filter(stage_info.stage)
        ]
        return IndexView(self, stage_infos, outs_filter=outs_filter)

    @property
    def outs(self) -> Iterator["Output"]:
        for stage in self:
            yield from stage.outs

    @property
    def decorated_outs(self) -> Iterator["Output"]:
        for output in self.outs:
            if output.is_decorated:
                yield output

    @property
    def metrics(self) -> Iterator["Output"]:
        for output in self.outs:
            if output.is_metric:
                yield output

    @property
    def plots(self) -> Iterator["Output"]:
        for output in self.outs:
            if output.is_plot:
                yield output

    @property
    def deps(self) -> Iterator["Dependency"]:
        for stage in self:
            yield from stage.deps

    @property
    def params(self) -> Iterator["ParamsDependency"]:
        from dvc.dependency import ParamsDependency

        for dep in self.deps:
            if isinstance(dep, ParamsDependency):
                yield dep

    @cached_property
    def outs_trie(self) -> "Trie":
        from dvc.repo.trie import build_outs_trie

        return build_outs_trie(self.stages)

    @cached_property
    def graph(self) -> "DiGraph":
        from dvc.repo.graph import build_graph

        return build_graph(self.stages, self.outs_trie)

    @cached_property
    def outs_graph(self) -> "DiGraph":
        from dvc.repo.graph import build_outs_graph

        return build_outs_graph(self.graph, self.outs_trie)

    @cached_property
    def data(self) -> "Dict[str, DataIndex]":
        from collections import defaultdict

        from dvc_data.index import DataIndex

        by_workspace: dict = defaultdict(DataIndex)

        by_workspace["repo"] = DataIndex()
        by_workspace["local"] = DataIndex()

        for out in self.outs:
            if not out.use_cache:
                continue

            workspace, key = out.index_key
            data_index = by_workspace[workspace]

            entry = out.get_entry()
            entry.key = key
            data_index.add(entry)

        return dict(by_workspace)

    def used_objs(
        self,
        targets: "TargetType" = None,
        with_deps: bool = False,
        remote: str = None,
        force: bool = False,
        recursive: bool = False,
        jobs: int = None,
    ) -> "ObjectContainer":
        from collections import defaultdict

        used: "ObjectContainer" = defaultdict(set)
        pairs = self._collect_targets(
            targets, recursive=recursive, with_deps=with_deps
        )
        for stage, filter_info in pairs:
            for odb, objs in stage.get_used_objs(
                remote=remote,
                force=force,
                jobs=jobs,
                filter_info=filter_info,
            ).items():
                used[odb].update(objs)
        return used

    # Following methods help us treat the collection as a set-like structure
    # and provides faux-immutability.
    # These methods do not preserve stages order.

    def update(self, stages: Iterable["Stage"]) -> "Index":
        new_stages = set(stages)
        # we remove existing stages with same hashes at first
        # and then re-add the new ones later.
        stages_set = (set(self.stages) - new_stages) | new_stages
        return Index(self.repo, self.fs, stages=list(stages_set))

    def add(self, stage: "Stage") -> "Index":
        return self.update([stage])

    def remove(
        self, stage: "Stage", ignore_not_existing: bool = False
    ) -> "Index":
        stages = self._discard_stage(
            stage, ignore_not_existing=ignore_not_existing
        )
        return Index(self.repo, self.fs, stages=stages)

    def discard(self, stage: "Stage") -> "Index":
        return self.remove(stage, ignore_not_existing=True)

    def difference(self, stages: Iterable["Stage"]) -> "Index":
        stages_set = set(self.stages) - set(stages)
        return Index(self.repo, self.fs, stages=list(stages_set))

    def _discard_stage(
        self, stage: "Stage", ignore_not_existing: bool = False
    ) -> List["Stage"]:
        stages = self.stages[:]
        ctx = suppress(ValueError) if ignore_not_existing else nullcontext()
        with ctx:
            stages.remove(stage)
        return stages

    def check_graph(self) -> None:
        if not getattr(self.repo, "_skip_graph_checks", False):
            self.graph  # pylint: disable=pointless-statement

    def dumpd(self) -> Dict[str, Dict]:
        def dump(stage: "Stage"):
            key = stage.path_in_repo
            try:
                key += ":" + stage.name  # type: ignore[attr-defined]
            except AttributeError:
                pass
            return key, stage.dumpd()

        return dict(dump(stage) for stage in self)

    @cached_property
    def identifier(self) -> str:
        """Unique identifier for the index.

        We can use this to optimize and skip opening some indices
        eg: on push/pull/fetch/gc --all-commits.

        Currently, it is unique to the platform (windows vs posix).
        """
        return dict_md5(self.dumpd())


class _DataPrefixes(NamedTuple):
    explicit: Set["DataIndexKey"]
    recursive: Set["DataIndexKey"]


class IndexView:
    """Read-only view of Index.data using filtered stages."""

    def __init__(  # pylint: disable=redefined-outer-name
        self,
        index: Index,
        stage_infos: Iterable["StageInfo"],
        outs_filter: Optional[Callable[["Output"], bool]],
    ):
        self._index = index
        self._stage_infos = stage_infos
        # NOTE: stage_infos might have the same stage multiple times but with
        # different filter_info
        self._stages = list({stage for stage, _ in stage_infos})
        self._outs_filter = outs_filter

    def __len__(self) -> int:
        return len(self._stages)

    def __contains__(self, stage: "Stage") -> bool:
        # as we are keeping stages inside a list, it might be slower.
        return stage in self._stages

    def __iter__(self) -> Iterator["Stage"]:
        yield from self._stages

    def __getitem__(self, item: str) -> "Stage":
        """Get a stage by its addressing attribute."""
        for stage in self:
            if stage.addressing == item:
                return stage
        raise KeyError(f"{item} - available stages are {self.stages}")

    @property
    def stages(self):
        return self._stages

    @property
    def deps(self) -> Iterator["Dependency"]:
        for stage in self:
            yield from stage.deps

    @property
    def _filtered_outs(self) -> Iterator[Tuple["Output", Optional[str]]]:
        for stage, filter_info in self._stage_infos:
            for out in stage.filter_outs(filter_info):
                if not self._outs_filter or self._outs_filter(out):
                    yield out, filter_info

    @property
    def outs(self) -> Iterator["Output"]:
        yield from {out for (out, _) in self._filtered_outs}

    @cached_property
    def _data_prefixes(self) -> Dict[str, "_DataPrefixes"]:
        from collections import defaultdict

        prefixes: Dict[str, "_DataPrefixes"] = defaultdict(
            lambda: _DataPrefixes(set(), set())
        )
        for out, filter_info in self._filtered_outs:
            workspace, key = out.index_key
            if filter_info and out.fs.path.isin(filter_info, out.fs_path):
                key = key + out.fs.path.relparts(filter_info, out.fs_path)
            if out.meta.isdir:
                prefixes[workspace].recursive.add(key)
            prefixes[workspace].explicit.update(
                key[:i] for i in range(len(key), 0, -1)
            )
        return prefixes

    @cached_property
    def data(self) -> "Dict[str, DataIndexView]":
        from functools import partial

        from dvc_data.index import view

        def key_filter(workspace: str, key: "DataIndexKey"):
            try:
                prefixes = self._data_prefixes[workspace]
                return key in prefixes.explicit or any(
                    key[: len(prefix)] == prefix
                    for prefix in prefixes.recursive
                )
            except KeyError:
                return False

        data = {}
        for workspace, data_index in self._index.data.items():
            data_index.load()
            data[workspace] = view(data_index, partial(key_filter, workspace))
        return data


if __name__ == "__main__":
    import logging

    from funcy import log_durations

    from dvc.logger import setup
    from dvc.repo import Repo

    setup(level=logging.TRACE)  # type: ignore[attr-defined]

    repo = Repo()
    index = Index(repo, repo.fs)
    print(index)
    with log_durations(print, "collecting stages"):
        # pylint: disable=pointless-statement
        print("no of stages", len(index.stages))
    with log_durations(print, "building graph"):
        index.graph  # pylint: disable=pointless-statement
    with log_durations(print, "calculating hash"):
        print(index.identifier)
    with log_durations(print, "updating"):
        index2 = index.update(index.stages)
    with log_durations(print, "calculating hash"):
        print(index2.identifier)
