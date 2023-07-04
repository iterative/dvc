import logging
from functools import partial
from typing import TYPE_CHECKING, Iterable, List, Optional, Set, Union, cast

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
        logger.warning(
            "%s is frozen. Its dependencies are not going to be reproduced.",
            stage,
        )

    ret = stage.reproduce(**kwargs)
    if ret and not kwargs.get("dry", False):
        stage.dump(update_pipeline=False)
    return ret


def _reproduce_stages(stages: List["Stage"], on_error=None, **kwargs) -> List["Stage"]:
    result: List["Stage"] = []
    for stage in stages:
        try:
            ret = _reproduce_stage(stage, **kwargs)
        except Exception as exc:  # noqa: BLE001, pylint: disable=broad-exception-caught
            if not callable(on_error):
                raise ReproductionError(stage.addressing) from exc
            on_error(exc, stage)
            continue

        if ret:
            result.append(ret)
        logger.info("")

    return result


def _remove_dependents_recursively(graph: "DiGraph", node: "Stage") -> Set["Stage"]:
    visited: Set["Stage"] = set()

    def dfs(n):
        succ = list(graph.successors(n))
        visited.update(succ)
        for v in succ:
            dfs(v)

    dfs(node)
    for n in visited:
        graph.remove_node(n)
    return visited


def handle_error(graph: "DiGraph", on_error: str, e: Exception, node: "Stage"):
    if on_error == "fail":
        raise ReproductionError(node.addressing) from e

    logger.warning(e)
    if on_error == "ignore":
        return

    assert on_error == "skip_dependents"
    if removed := _remove_dependents_recursively(graph, node):
        logger.warning(
            "Stage%s %s will be skipped due to the above failure.",
            "s" if len(removed) > 1 else "",
            humanize.join([f"'{node.addressing}'" for node in removed]),
        )


def _reproduce_graph(
    graph: "DiGraph", force_downstream: bool = False, on_error: str = "fail", **kwargs
) -> List["Stage"]:
    assert on_error in ("fail", "skip_dependents", "ignore")
    g = cast("DiGraph", graph.reverse())
    stages: List["Stage"] = []
    result: List["Stage"] = []

    err_handler = partial(handle_error, g, on_error)
    while g:
        roots = [node for node, degree in g.in_degree() if degree == 0]
        ret = _reproduce_stages(roots, upstream=stages, on_error=err_handler, **kwargs)
        for node in roots:
            g.remove_node(node)

        result.extend(ret)
        stages.extend(roots)
        if ret and force_downstream:
            # NOTE: we are walking our pipeline from the top to the
            # bottom. If one stage is changed, it will be reproduced,
            # which tells us that we should force reproducing all of
            # the other stages down below, even if their direct
            # dependencies didn't change.
            kwargs["force"] = True
    return result


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
):
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
        return _reproduce_stages(stages, **kwargs)

    graph = self.index.graph
    active = get_active_graph(graph, stages, pipeline=pipeline, downstream=downstream)
    return _reproduce_graph(active, on_error=on_error, **kwargs)
