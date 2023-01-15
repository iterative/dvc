import logging
import time
from functools import partial
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
    Union,
)

from funcy.debug import format_time

from dvc.fs import LocalFileSystem
from dvc.utils.objects import cached_property

if TYPE_CHECKING:
    from networkx import DiGraph
    from pygtrie import Trie

    from dvc.dependency import Dependency, ParamsDependency
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.repo.stage import StageInfo
    from dvc.stage import Stage
    from dvc.types import TargetType
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.index import DataIndex, DataIndexKey, DataIndexView
    from dvc_objects.db import ObjectDB
    from dvc_objects.fs.base import FileSystem


logger = logging.getLogger(__name__)
ObjectContainer = Dict[Optional["ObjectDB"], Set["HashInfo"]]


def log_walk(seq):
    for root, dirs, files in seq:
        start = time.perf_counter()
        yield root, dirs, files
        duration = format_time(time.perf_counter() - start)
        logger.trace(  # type: ignore[attr-defined]
            "%s in collecting stages from %s", duration, root
        )


def collect_files(
    repo: "Repo", onerror: Optional[Callable[[str, Exception], None]] = None
):
    """Collects all of the stages present in the DVC repo.

    Args:
        onerror (optional): callable that will be called with two args:
            the filepath whose collection failed and the exc instance.
            It can report the error to continue with the collection
            (and, skip failed ones), or raise the exception to abort
            the collection.
    """
    from dvc.dvcfile import is_valid_filename
    from dvc.exceptions import DvcException
    from dvc.utils import relpath

    scm = repo.scm
    fs = repo.fs
    sep = fs.sep
    outs: Set[str] = set()

    is_local_fs = isinstance(fs, LocalFileSystem)

    def is_ignored(path):
        # apply only for the local fs
        return is_local_fs and scm.is_ignored(path)

    def is_dvcfile_and_not_ignored(root, file):
        return is_valid_filename(file) and not is_ignored(f"{root}{sep}{file}")

    def is_out_or_ignored(root, directory):
        dir_path = f"{root}{sep}{directory}"
        # trailing slash needed to check if a directory is gitignored
        return dir_path in outs or is_ignored(f"{dir_path}{sep}")

    walk_iter = repo.dvcignore.walk(fs, repo.root_dir, followlinks=False)
    if logger.isEnabledFor(logging.TRACE):  # type: ignore[attr-defined]
        walk_iter = log_walk(walk_iter)

    for root, dirs, files in walk_iter:
        dvcfile_filter = partial(is_dvcfile_and_not_ignored, root)
        for file in filter(dvcfile_filter, files):
            file_path = fs.path.join(root, file)
            try:
                index = Index.from_file(repo, file_path)
            except DvcException as exc:
                if onerror:
                    onerror(relpath(file_path), exc)
                    continue
                raise

            outs.update(
                out.fspath
                for stage in index.stages
                for out in stage.outs
                if out.protocol == "local"
            )
            yield file_path, index
        dirs[:] = [d for d in dirs if not is_out_or_ignored(root, d)]


class Index:
    def __init__(
        self,
        repo: "Repo",
        stages: Optional[List["Stage"]] = None,
        metrics: Optional[Dict[str, List[str]]] = None,
        plots: Optional[Dict[str, List[str]]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.repo = repo
        self.stages = stages or []
        self._metrics = metrics or {}
        self._plots = plots or {}
        self._params = params or {}
        self._collected_targets: Dict[int, List["StageInfo"]] = {}

    def __repr__(self) -> str:
        rev = "workspace"
        if not isinstance(self.repo.fs, LocalFileSystem):
            rev = self.repo.get_rev()[:7]
        return f"Index({self.repo}, fs@{rev})"

    @classmethod
    def from_repo(
        cls,
        repo: "Repo",
        onerror: Optional[Callable[[str, Exception], None]] = None,
    ) -> "Index":
        stages = []
        metrics = {}
        plots = {}
        params = {}

        onerror = onerror or repo.stage_collection_error_handler
        for _, idx in collect_files(repo, onerror=onerror):
            # pylint: disable=protected-access
            stages.extend(idx.stages)
            metrics.update(idx._metrics)
            plots.update(idx._plots)
            params.update(idx._params)
        return cls(
            repo,
            stages=stages,
            metrics=metrics,
            plots=plots,
            params=params,
        )

    @classmethod
    def from_file(cls, repo: "Repo", path: str) -> "Index":
        from dvc.dvcfile import load_file

        dvcfile = load_file(repo, path)
        return cls(
            repo,
            stages=list(dvcfile.stages.values()),
            metrics={path: dvcfile.metrics} if dvcfile.metrics else {},
            plots={path: dvcfile.plots} if dvcfile.plots else {},
            params={path: dvcfile.params} if dvcfile.params else {},
        )

    def update(self, stages: Iterable["Stage"]) -> "Index":
        stages = set(stages)
        # we remove existing stages with same hashes at first
        # and then re-add the new ones later.
        stages_set = (set(self.stages) - stages) | stages
        return self.__class__(
            self.repo,
            stages=list(stages_set),
            metrics=self._metrics,
            plots=self._plots,
            params=self._params,
        )

    @cached_property
    def outs_trie(self) -> "Trie":
        from dvc.repo.trie import build_outs_trie

        return build_outs_trie(self.stages)

    @cached_property
    def outs_graph(self) -> "DiGraph":
        from dvc.repo.graph import build_outs_graph

        return build_outs_graph(self.graph, self.outs_trie)

    @cached_property
    def graph(self) -> "DiGraph":
        from dvc.repo.graph import build_graph

        return build_graph(self.stages, self.outs_trie)

    def check_graph(self) -> None:
        if not getattr(self.repo, "_skip_graph_checks", False):
            self.graph  # pylint: disable=pointless-statement

    @property
    def params(self) -> Iterator["ParamsDependency"]:
        from dvc.dependency import ParamsDependency

        for dep in self.deps:
            if isinstance(dep, ParamsDependency):
                yield dep

    @property
    def outs(self) -> Iterator["Output"]:
        for stage in self.stages:
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
        for stage in self.stages:
            yield from stage.deps

    @cached_property
    def _plot_sources(self) -> List[str]:
        from dvc.repo.plots import _collect_pipeline_files

        sources: List[str] = []
        for data in _collect_pipeline_files(self.repo, [], {}).values():
            for plot_id, props in data.get("data", {}).items():
                if isinstance(props.get("y"), dict):
                    sources.extend(props["y"])
                    if isinstance(props.get("x"), dict):
                        sources.extend(props["x"])
                else:
                    sources.append(plot_id)
        return sources

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

    def collect_targets(
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
                    self.repo.stage.collect_granular(target, **kwargs)
                    for target in targets
                )
            )
        return self._collected_targets[targets_hash]

    def used_objs(
        self,
        targets: Optional["TargetType"] = None,
        with_deps: bool = False,
        remote: Optional[str] = None,
        force: bool = False,
        recursive: bool = False,
        jobs: Optional[int] = None,
        push: bool = False,
    ) -> "ObjectContainer":
        from collections import defaultdict

        used: "ObjectContainer" = defaultdict(set)
        pairs = self.collect_targets(
            targets, recursive=recursive, with_deps=with_deps
        )
        for stage, filter_info in pairs:
            for odb, objs in stage.get_used_objs(
                remote=remote,
                force=force,
                jobs=jobs,
                filter_info=filter_info,
                push=push,
            ).items():
                used[odb].update(objs)
        return used

    def targets_view(
        self,
        targets: Optional["TargetType"],
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
            for stage_info in self.collect_targets(targets, **kwargs)
            if not stage_filter or stage_filter(stage_info.stage)
        ]
        return IndexView(self, stage_infos, outs_filter=outs_filter)


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
        self.stages = list({stage for stage, _ in stage_infos})
        self._outs_filter = outs_filter

    @property
    def deps(self) -> Iterator["Dependency"]:
        for stage in self.stages:
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
    def data(self) -> Dict[str, Union["DataIndex", "DataIndexView"]]:
        from dvc_data.index import DataIndex, view

        def key_filter(workspace: str, key: "DataIndexKey"):
            try:
                prefixes = self._data_prefixes[workspace]
                return key in prefixes.explicit or any(
                    key[: len(prefix)] == prefix
                    for prefix in prefixes.recursive
                )
            except KeyError:
                return False

        data: Dict[str, Union["DataIndex", "DataIndexView"]] = {}
        for workspace, data_index in self._index.data.items():
            if self.stages:
                data[workspace] = view(
                    data_index, partial(key_filter, workspace)
                )
            else:
                data[workspace] = DataIndex()
        return data


def build_data_index(
    index: Union["Index", "IndexView"],
    path: str,
    fs: "FileSystem",
    workspace: Optional[str] = "repo",
) -> "DataIndex":
    from dvc_data.index import DataIndex, DataIndexEntry
    from dvc_data.index.build import build_entries, build_entry

    data = DataIndex()
    for out in index.outs:
        if not out.use_cache:
            continue

        ws, key = out.index_key
        if ws != workspace:
            continue

        parts = out.fs.path.relparts(out.fs_path, out.repo.root_dir)
        out_path = fs.path.join(path, *parts)

        try:
            entry = build_entry(out_path, fs)
        except FileNotFoundError:
            entry = DataIndexEntry(path=out_path, fs=fs)

        entry.key = key

        if not entry.meta or not entry.meta.isdir:
            data.add(entry)
            continue

        entry.loaded = True
        data.add(entry)

        for entry in build_entries(out_path, fs):
            if not entry.key or entry.key == ("",):
                # NOTE: whether the root will be returned by build_entries
                # depends on the filesystem (e.g. local doesn't, but s3 does).
                entry.key = key
            else:
                entry.key = key + entry.key
            data.add(entry)

    return data
