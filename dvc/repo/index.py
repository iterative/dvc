import logging
import time
from collections import defaultdict
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
from dvc.fs.callbacks import DEFAULT_CALLBACK
from dvc.utils.objects import cached_property

if TYPE_CHECKING:
    from networkx import DiGraph
    from pygtrie import Trie

    from dvc.dependency import Dependency, ParamsDependency
    from dvc.fs.callbacks import Callback
    from dvc.output import Output
    from dvc.repo import Repo
    from dvc.repo.stage import StageInfo
    from dvc.stage import Stage
    from dvc.types import TargetType
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.index import DataIndex, DataIndexKey, DataIndexView
    from dvc_objects.fs.base import FileSystem


logger = logging.getLogger(__name__)
ObjectContainer = Dict[Optional["HashFileDB"], Set["HashInfo"]]


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


def _load_data_from_outs(index, prefix, outs):
    from dvc_data.index import DataIndexEntry, Meta

    parents = set()
    for out in outs:
        if not out.use_cache:
            continue

        ws, key = out.index_key

        for key_len in range(1, len(key)):
            parents.add((ws, key[:key_len]))

        entry = DataIndexEntry(
            key=key,
            meta=out.meta,
            hash_info=out.hash_info,
        )

        if out.stage.is_import and not out.stage.is_repo_import:
            dep = out.stage.deps[0]
            entry.meta = dep.meta
            if out.hash_info:
                entry.hash_info = out.hash_info
            else:
                # partial import
                entry.hash_info = dep.hash_info

        # FIXME PyGTrie-based DataIndex doesn't remove entry.key during
        # index.add, so we have to set the entry manually here to make
        # index.view() work correctly.
        index[(*prefix, ws, *key)] = entry

    for ws, key in parents:
        index[(*prefix, ws, *key)] = DataIndexEntry(
            key=key, meta=Meta(isdir=True), loaded=True
        )


def _load_storage_from_out(storage_map, key, out):
    from dvc.cachemgr import LEGACY_HASH_NAMES
    from dvc.config import NoRemoteError
    from dvc_data.index import FileStorage, ObjectStorage

    if out.odb:
        storage_map.add_data(ObjectStorage(key, out.odb))
    storage_map.add_cache(ObjectStorage(key, out.cache))
    try:
        remote = out.repo.cloud.get_remote(out.remote)
        if remote.fs.version_aware:
            storage_map.add_remote(
                FileStorage(
                    key=key,
                    fs=remote.fs,
                    path=remote.path,
                    index=remote.index,
                    prefix=(),
                )
            )
        else:
            odb = (
                remote.legacy_odb if out.hash_name in LEGACY_HASH_NAMES else remote.odb
            )
            storage_map.add_remote(ObjectStorage(key, odb, index=remote.index))
    except NoRemoteError:
        pass

    if out.stage.is_import:
        dep = out.stage.deps[0]
        if not out.hash_info:
            from fsspec.utils import tokenize

            # partial import
            fs_cache = out.repo.cache.fs_cache
            storage_map.add_cache(
                FileStorage(
                    key,
                    fs_cache.fs,
                    fs_cache.fs.path.join(
                        fs_cache.path, dep.fs.protocol, tokenize(dep.fs_path)
                    ),
                )
            )
        storage_map.add_remote(FileStorage(key, dep.fs, dep.fs_path))


class Index:
    def __init__(
        self,
        repo: "Repo",
        stages: Optional[List["Stage"]] = None,
        metrics: Optional[Dict[str, List[str]]] = None,
        plots: Optional[Dict[str, List[str]]] = None,
        params: Optional[Dict[str, Any]] = None,
        artifacts: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.repo = repo
        self.stages = stages or []
        self._metrics = metrics or {}
        self._plots = plots or {}
        self._params = params or {}
        self._artifacts = artifacts or {}
        self._collected_targets: Dict[int, List["StageInfo"]] = {}

    @cached_property
    def rev(self) -> Optional[str]:
        if not isinstance(self.repo.fs, LocalFileSystem):
            return self.repo.get_rev()[:7]
        return None

    def __repr__(self) -> str:
        rev = self.rev or "workspace"
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
        artifacts = {}

        onerror = onerror or repo.stage_collection_error_handler
        for _, idx in collect_files(repo, onerror=onerror):
            # pylint: disable=protected-access
            stages.extend(idx.stages)
            metrics.update(idx._metrics)
            plots.update(idx._plots)
            params.update(idx._params)
            artifacts.update(idx._artifacts)
        return cls(
            repo,
            stages=stages,
            metrics=metrics,
            plots=plots,
            params=params,
            artifacts=artifacts,
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
            artifacts={path: dvcfile.artifacts} if dvcfile.artifacts else {},
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
            artifacts=self._artifacts,
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
            self.graph  # noqa: B018, pylint: disable=pointless-statement

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

    @cached_property
    def out_data_keys(self) -> Dict[str, Set["DataIndexKey"]]:
        by_workspace: Dict[str, Set["DataIndexKey"]] = defaultdict(set)

        by_workspace["repo"] = set()
        by_workspace["local"] = set()

        for out in self.outs:
            if not out.use_cache:
                continue

            ws, key = out.index_key
            by_workspace[ws].add(key)

        return dict(by_workspace)

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
    def data_keys(self) -> Dict[str, Set["DataIndexKey"]]:
        by_workspace: Dict[str, Set["DataIndexKey"]] = defaultdict(set)

        by_workspace["repo"] = set()
        by_workspace["local"] = set()

        for out in self.outs:
            if not out.use_cache:
                continue

            workspace, key = out.index_key
            by_workspace[workspace].add(key)

        return dict(by_workspace)

    @cached_property
    def metric_keys(self) -> Dict[str, Set["DataIndexKey"]]:
        from .metrics.show import _collect_top_level_metrics

        by_workspace: Dict[str, Set["DataIndexKey"]] = defaultdict(set)

        by_workspace["repo"] = set()

        for out in self.outs:
            if not out.metric:
                continue

            workspace, key = out.index_key
            by_workspace[workspace].add(key)

        for path in _collect_top_level_metrics(self.repo):
            key = self.repo.fs.path.relparts(path, self.repo.root_dir)
            by_workspace["repo"].add(key)

        return dict(by_workspace)

    @cached_property
    def plot_keys(self) -> Dict[str, Set["DataIndexKey"]]:
        by_workspace: Dict[str, Set["DataIndexKey"]] = defaultdict(set)

        by_workspace["repo"] = set()

        for out in self.outs:
            if not out.plot:
                continue

            workspace, key = out.index_key
            by_workspace[workspace].add(key)

        for path in self._plot_sources:
            key = self.repo.fs.path.parts(path)
            by_workspace["repo"].add(key)

        return dict(by_workspace)

    @cached_property
    def data_tree(self):
        from dvc_data.hashfile.tree import Tree

        tree = Tree()
        for out in self.outs:
            if not out.use_cache:
                continue

            ws, key = out.index_key

            tree.add((ws, *key), out.meta, out.hash_info)

        tree.digest()

        return tree

    @cached_property
    def data(self) -> "Dict[str, DataIndex]":
        prefix: "DataIndexKey"
        loaded = False

        index = self.repo.data_index
        prefix = ("tree", self.data_tree.hash_info.value)
        if index.has_node(prefix):
            loaded = True

        if not loaded:
            _load_data_from_outs(index, prefix, self.outs)
            index.commit()

        by_workspace = {}
        by_workspace["repo"] = index.view((*prefix, "repo"))
        by_workspace["local"] = index.view((*prefix, "local"))

        for out in self.outs:
            if not out.use_cache:
                continue

            if not out.is_in_repo:
                continue

            ws, key = out.index_key
            if ws not in by_workspace:
                by_workspace[ws] = index.view((*prefix, ws))

            data_index = by_workspace[ws]
            _load_storage_from_out(data_index.storage_map, key, out)

        return by_workspace

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
        self, targets: Optional["TargetType"], *, onerror=None, **kwargs: Any
    ) -> List["StageInfo"]:
        from dvc.exceptions import DvcException
        from dvc.repo.stage import StageInfo
        from dvc.utils.collections import ensure_list

        if not onerror:

            def onerror(_target, _exc):
                raise  # pylint: disable=misplaced-bare-raise

        targets = ensure_list(targets)
        if not targets:
            return [StageInfo(stage) for stage in self.stages]
        targets_hash = self._hash_targets(targets, **kwargs)
        if targets_hash not in self._collected_targets:
            collected = []
            for target in targets:
                try:
                    collected.extend(self.repo.stage.collect_granular(target, **kwargs))
                except DvcException as exc:
                    onerror(target, exc)
            self._collected_targets[targets_hash] = collected

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
        used: "ObjectContainer" = defaultdict(set)
        pairs = self.collect_targets(targets, recursive=recursive, with_deps=with_deps)
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

    def _types_filter(self, types, out):
        ws, okey = out.index_key
        for typ in types:
            if typ == "plots":
                keys = self.plot_keys
            elif typ == "metrics":
                keys = self.metric_keys
            else:
                raise ValueError(f"unsupported type {typ}")

            for key in keys.get(ws, []):
                if (len(key) >= len(okey) and key[: len(okey)] == okey) or (
                    len(key) < len(okey) and okey[: len(key)] == key
                ):
                    return True

        return False

    def targets_view(
        self,
        targets: Optional["TargetType"],
        stage_filter: Optional[Callable[["Stage"], bool]] = None,
        outs_filter: Optional[Callable[["Output"], bool]] = None,
        max_size: Optional[int] = None,
        types: Optional[List[str]] = None,
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

        def _outs_filter(out):
            if max_size and out.meta and out.meta.size and out.meta.size >= max_size:
                return False

            if types and not self._types_filter(types, out):
                return False

            if outs_filter:
                return outs_filter(out)

            return True

        return IndexView(self, stage_infos, outs_filter=_outs_filter)


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
    def repo(self) -> "Repo":
        return self._index.repo

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
    def out_data_keys(self) -> Dict[str, Set["DataIndexKey"]]:
        by_workspace: Dict[str, Set["DataIndexKey"]] = defaultdict(set)

        by_workspace["repo"] = set()
        by_workspace["local"] = set()

        for out in self.outs:
            if not out.use_cache:
                continue

            ws, key = out.index_key
            by_workspace[ws].add(key)

        return dict(by_workspace)

    @cached_property
    def _data_prefixes(self) -> Dict[str, "_DataPrefixes"]:
        prefixes: Dict[str, "_DataPrefixes"] = defaultdict(
            lambda: _DataPrefixes(set(), set())
        )
        for out, filter_info in self._filtered_outs:
            if not out.use_cache:
                continue
            workspace, key = out.index_key
            if filter_info and out.fs.path.isin(filter_info, out.fs_path):
                key = key + out.fs.path.relparts(filter_info, out.fs_path)
            entry = self._index.data[workspace].get(key)
            if entry and entry.meta and entry.meta.isdir:
                prefixes[workspace].recursive.add(key)
            prefixes[workspace].explicit.update(key[:i] for i in range(len(key), 0, -1))
        return prefixes

    @cached_property
    def data_keys(self) -> Dict[str, Set["DataIndexKey"]]:
        ret: Dict[str, Set["DataIndexKey"]] = defaultdict(set)

        for out, filter_info in self._filtered_outs:
            if not out.use_cache:
                continue

            workspace, key = out.index_key
            if filter_info and out.fs.path.isin(filter_info, out.fs_path):
                key = key + out.fs.path.relparts(filter_info, out.fs_path)
            ret[workspace].add(key)

        return dict(ret)

    @cached_property
    def data_tree(self):
        from dvc_data.hashfile.tree import Tree

        tree = Tree()
        for out in self.outs:
            if not out.use_cache:
                continue

            ws, key = out.index_key

            tree.add((ws, *key), out.meta, out.hash_info)

        tree.digest()

        return tree

    @cached_property
    def data(self) -> Dict[str, Union["DataIndex", "DataIndexView"]]:
        from dvc_data.index import DataIndex, view

        def key_filter(workspace: str, key: "DataIndexKey"):
            try:
                prefixes = self._data_prefixes[workspace]
                return key in prefixes.explicit or any(
                    key[: len(prefix)] == prefix for prefix in prefixes.recursive
                )
            except KeyError:
                return False

        data: Dict[str, Union["DataIndex", "DataIndexView"]] = {}
        for workspace, data_index in self._index.data.items():
            if self.stages:
                data[workspace] = view(data_index, partial(key_filter, workspace))
            else:
                data[workspace] = DataIndex()
        return data


def build_data_index(  # noqa: C901, PLR0912
    index: Union["Index", "IndexView"],
    path: str,
    fs: "FileSystem",
    workspace: str = "repo",
    compute_hash: Optional[bool] = False,
    callback: "Callback" = DEFAULT_CALLBACK,
) -> "DataIndex":
    from dvc_data.index import DataIndex, DataIndexEntry, Meta
    from dvc_data.index.build import build_entries, build_entry
    from dvc_data.index.save import build_tree

    ignore = None
    if workspace == "repo" and isinstance(fs, LocalFileSystem):
        ignore = index.repo.dvcignore

    data = DataIndex()
    parents = set()
    for key in index.data_keys.get(workspace, set()):
        out_path = fs.path.join(path, *key)

        for key_len in range(1, len(key)):
            parents.add(key[:key_len])

        if not fs.exists(out_path):
            continue

        hash_name = _get_entry_hash_name(index, workspace, key)
        try:
            out_entry = build_entry(
                out_path,
                fs,
                compute_hash=compute_hash,
                state=index.repo.state,
                hash_name=hash_name,
            )
        except FileNotFoundError:
            out_entry = DataIndexEntry()

        out_entry.key = key
        data.add(out_entry)
        callback.relative_update(1)

        if not out_entry.meta or not out_entry.meta.isdir:
            continue

        for entry in build_entries(
            out_path,
            fs,
            compute_hash=compute_hash,
            state=index.repo.state,
            ignore=ignore,
            hash_name=hash_name,
        ):
            if not entry.key or entry.key == ("",):
                # NOTE: whether the root will be returned by build_entries
                # depends on the filesystem (e.g. local doesn't, but s3 does).
                continue

            entry.key = key + entry.key
            data.add(entry)
            callback.relative_update(1)

    for key in parents:
        parent_path = fs.path.join(path, *key)
        if not fs.exists(parent_path):
            continue
        direntry = DataIndexEntry(key=key, meta=Meta(isdir=True), loaded=True)
        data.add(direntry)
        callback.relative_update(1)

    if compute_hash:
        out_keys = index.out_data_keys.get(workspace, set())
        data_keys = index.data_keys.get(workspace, set())
        for key in data_keys.intersection(out_keys):
            hash_name = _get_entry_hash_name(index, workspace, key)

            out_entry = data.get(key)
            if not out_entry or not out_entry.isdir:
                continue

            tree_meta, tree = build_tree(data, key, name=hash_name)
            out_entry.meta = tree_meta
            out_entry.hash_info = tree.hash_info
            out_entry.loaded = True
            data.add(out_entry)
            callback.relative_update(1)

    return data


def _get_entry_hash_name(
    index: Union["Index", "IndexView"], workspace: str, key: "DataIndexKey"
) -> str:
    from dvc_data.hashfile.hash import DEFAULT_ALGORITHM

    for idx in reversed(range(len(key) + 1)):
        prefix = key[:idx]
        try:
            src_entry = index.data[workspace][prefix]
        except KeyError:
            continue

        if src_entry.hash_info and src_entry.hash_info.name:
            return src_entry.hash_info.name

    return DEFAULT_ALGORITHM
