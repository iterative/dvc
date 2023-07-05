import logging
from dataclasses import dataclass, field
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
    cast,
)

from funcy import ldistinct

from dvc.exceptions import ReproductionError
from dvc.repo.scm_context import scm_context
from dvc.stage.cache import RunCacheNotSupported
from dvc.utils import humanize
from dvc.utils.collections import ensure_list

from . import locked

if TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.stage import Stage

    from . import Repo

logger = logging.getLogger(__name__)

T = TypeVar("T")


def collect_stages(
    repo: "Repo",
    targets: Iterable[str],
    recursive: bool = False,
    glob: bool = False,
) -> List["Stage"]:
    stages: List["Stage"] = []
    for target in targets:
        stages.extend(repo.stage.collect(target, recursive=recursive, glob=glob))
    return ldistinct(stages)


def get_subgraph(
    graph: "DiGraph",
    nodes: Optional[List] = None,
    pipeline: bool = False,
    downstream: bool = False,
) -> "DiGraph":
    import networkx as nx

    from .graph import get_pipeline, get_pipelines, get_subgraph_of_nodes

    if not pipeline or not nodes:
        return get_subgraph_of_nodes(graph, nodes, downstream=downstream)

    pipelines = get_pipelines(graph)
    used_pipelines = [get_pipeline(pipelines, node) for node in nodes]
    return nx.compose_all(used_pipelines)


def _remove_frozen_stages(graph: "DiGraph") -> "DiGraph":
    g = cast("DiGraph", graph.copy())
    for stage in graph:
        if stage.frozen:
            # NOTE: disconnect frozen stage from its dependencies
            g.remove_edges_from(graph.out_edges(stage))
    return g


def get_active_graph(
    graph: "DiGraph",
    stages: Optional[List["Stage"]] = None,
    pipeline: bool = False,
    downstream: bool = False,
) -> "DiGraph":
    """Return the graph to operate."""
    processed = _remove_frozen_stages(graph)
    return get_subgraph(processed, stages, pipeline=pipeline, downstream=downstream)


def plan_repro(
    graph: "DiGraph",
    stages: Optional[List["Stage"]] = None,
    pipeline: bool = False,
    downstream: bool = False,
) -> List["Stage"]:
    r"""Derive the evaluation of the given node for the given graph.

    When you _reproduce a stage_, you want to _evaluate the descendants_
    to know if it make sense to _recompute_ it. A post-ordered search
    will give us an order list of the nodes we want.

    For example, let's say that we have the following pipeline:

                               E
                              / \
                             D   F
                            / \   \
                           B   C   G
                            \ /
                             A

    The derived evaluation of D would be: [A, B, C, D]

    In case that `downstream` option is specified, the desired effect
    is to derive the evaluation starting from the given stage up to the
    ancestors. However, the `networkx.ancestors` returns a set, without
    any guarantee of any order, so we are going to reverse the graph and
    use a reverse post-ordered search using the given stage as a starting
    point.

                   E                                   A
                  / \                                 / \
                 D   F                               B   C   G
                / \   \        --- reverse -->        \ /   /
               B   C   G                               D   F
                \ /                                     \ /
                 A                                       E

    The derived evaluation of _downstream_ B would be: [B, D, E]
    """
    import networkx as nx

    active = get_active_graph(graph, stages, pipeline=pipeline, downstream=downstream)
    return list(nx.dfs_postorder_nodes(active))


def _reproduce_stage(stage: "Stage", **kwargs) -> Optional["Stage"]:
    if stage.frozen and not stage.is_import:
        msg = "%s is frozen. Its dependencies are not going to be reproduced."
        logger.warning(msg, stage)

    ret = stage.reproduce(**kwargs)
    if ret and not kwargs.get("dry", False):
        stage.dump(update_pipeline=False)
    return ret


@dataclass
class Stats:
    reproduced: List["Stage"] = field(default_factory=list)
    failed: List["Stage"] = field(default_factory=list)
    unchanged: List["Stage"] = field(default_factory=list)

    def merge(self, other: "Stats") -> None:
        self.reproduced.extend(other.reproduced)
        self.failed.extend(other.failed)
        self.unchanged.extend(other.unchanged)


def _reproduce_stages(
    stages: List["Stage"],
    on_error: Optional[Callable[[Exception, "Stage"], Any]] = None,
    **kwargs,
) -> Stats:
    stats = Stats()

    for stage in stages:
        try:
            ret = _reproduce_stage(stage, **kwargs)
        except Exception as e:  # noqa: BLE001, pylint: disable=broad-exception-caught
            stats.failed.append(stage)
            if callable(on_error):
                on_error(e, stage)
                continue
            raise ReproductionError(f"failed to reproduce '{stage.addressing}'") from e

        if not ret:
            stats.unchanged.append(stage)
            continue

        stats.reproduced.append(ret)
        logger.info("")
    return stats


def _remove_dependents_recursively(graph: "DiGraph", node: T) -> Set[T]:
    visited: Set[T] = set()

    def dfs(n):
        succ = list(graph.successors(n))
        visited.update(succ)
        for v in succ:
            dfs(v)

    dfs(node)
    for n in visited:
        graph.remove_node(n)
    return visited


def _stage_names(*stages: "Stage") -> str:
    return humanize.join([repr(stage.addressing) for stage in stages])


def handle_error(
    graph: "DiGraph", on_error: str, e: Exception, node: "Stage"
) -> Set["Stage"]:
    logger.warning(e)
    if on_error == "ignore":
        return {node}

    assert on_error == "skip_dependents"
    if removed := _remove_dependents_recursively(graph, node):
        msg = "Stage%s %s will be skipped due to the above failure."
        logger.warning(msg, "s" if len(removed) > 1 else "", _stage_names(*removed))
    return removed


def topological_generations(graph: "DiGraph") -> Iterator[List]:
    # Allows modifying graph while iterating, and consumes node as it goes.
    while graph:
        roots = [node for node, degree in graph.in_degree() if degree == 0]
        yield roots

        for node in roots:
            graph.remove_node(node)


def _reproduce_graph(
    graph: "DiGraph", force_downstream: bool = False, on_error: str = "fail", **kwargs
) -> List["Stage"]:
    assert on_error in ("fail", "skip_dependents", "ignore")
    g = cast("DiGraph", graph.reverse())
    err_handler = None if on_error == "fail" else partial(handle_error, g, on_error)
    all_stats = Stats()

    for gen in topological_generations(g):
        stages = all_stats.reproduced + all_stats.unchanged
        stats = _reproduce_stages(gen, upstream=stages, on_error=err_handler, **kwargs)
        if stats.reproduced and force_downstream:
            # NOTE: we are walking our pipeline from the top to the
            # bottom. If one stage is changed, it will be reproduced,
            # which tells us that we should force reproducing all of
            # the other stages down below, even if their direct
            # dependencies didn't change.
            kwargs["force"] = True
        all_stats.merge(stats)

    if failed := all_stats.failed:
        raise ReproductionError("failed to reproduce stages: " + _stage_names(*failed))
    return all_stats.reproduced


@locked
@scm_context
def reproduce(
    self: "Repo",
    targets: Union[Iterable[str], str, None] = None,
    recursive: bool = False,
    pipeline: bool = False,
    all_pipelines: bool = False,
    downstream: bool = False,
    single_item: bool = False,
    glob: bool = False,
    on_error: str = "fail",
    **kwargs,
) -> List["Stage"]:
    from dvc.dvcfile import PROJECT_FILE

    if all_pipelines or pipeline:
        single_item = False
        downstream = False

    if not kwargs.get("interactive", False):
        kwargs["interactive"] = self.config["core"].get("interactive", False)

    stages: List["Stage"] = []
    if not all_pipelines:
        targets_list = ensure_list(targets or PROJECT_FILE)
        stages = collect_stages(self, targets_list, recursive=recursive, glob=glob)

    if kwargs.get("pull", False) and kwargs.get("run_cache", True):
        logger.debug("Pulling run cache")
        try:
            self.stage_cache.pull(None)
        except RunCacheNotSupported as e:
            logger.warning("Failed to pull run cache: %s", e)

    if single_item:
        stats = _reproduce_stages(stages, **kwargs)
        assert not stats.failed
        return stats.reproduced

    graph = self.index.graph
    active = get_active_graph(graph, stages, pipeline=pipeline, downstream=downstream)
    return _reproduce_graph(active, on_error=on_error, **kwargs)
