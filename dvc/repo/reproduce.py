from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    NoReturn,
    Optional,
    Set,
    Tuple,
    TypeVar,
    Union,
    cast,
)

from funcy import ldistinct

from dvc.exceptions import ReproductionError
from dvc.log import logger
from dvc.repo.scm_context import scm_context
from dvc.stage.cache import RunCacheNotSupported
from dvc.utils import humanize
from dvc.utils.collections import ensure_list

from . import locked

if TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.stage import Stage

    from . import Repo

logger = logger.getChild(__name__)
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


def get_active_graph(graph: "DiGraph") -> "DiGraph":
    g = cast("DiGraph", graph.copy())
    for stage in graph:
        if stage.frozen:
            # NOTE: disconnect frozen stage from its dependencies
            g.remove_edges_from(graph.out_edges(stage))
    return g


def plan_repro(
    graph: "DiGraph",
    stages: Optional[List["T"]] = None,
    pipeline: bool = False,
    downstream: bool = False,
) -> List["T"]:
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

    sub = get_subgraph(graph, stages, pipeline=pipeline, downstream=downstream)
    return list(nx.dfs_postorder_nodes(sub))


def _reproduce_stage(stage: "Stage", **kwargs) -> Optional["Stage"]:
    if stage.frozen and not stage.is_import:
        msg = "%s is frozen. Its dependencies are not going to be reproduced."
        logger.warning(msg, stage)

    ret = stage.reproduce(**kwargs)
    if ret and not kwargs.get("dry", False):
        stage.dump(update_pipeline=False)
    return ret


def _get_upstream_downstream_nodes(
    graph: Optional["DiGraph"], node: T
) -> Tuple[List[T], List[T]]:
    succ = list(graph.successors(node)) if graph else []
    pre = list(graph.predecessors(node)) if graph else []
    return succ, pre


def _repr(stages: Iterable["Stage"]) -> str:
    return humanize.join(repr(stage.addressing) for stage in stages)


def handle_error(
    graph: Optional["DiGraph"], on_error: str, exc: Exception, stage: "Stage"
) -> Set["Stage"]:
    import networkx as nx

    logger.warning("%s%s", exc, " (ignored)" if on_error == "ignore" else "")
    if not graph or on_error == "ignore":
        return set()

    dependents = set(nx.dfs_postorder_nodes(graph.reverse(), stage)) - {stage}
    if dependents:
        names = _repr(dependents)
        msg = "%s %s will be skipped due to this failure"
        logger.warning(msg, "Stages" if len(dependents) > 1 else "Stage", names)
    return dependents


def _raise_error(exc: Optional[Exception], *stages: "Stage") -> NoReturn:
    names = _repr(stages)
    segment = " stages:" if len(stages) > 1 else ""
    raise ReproductionError(f"failed to reproduce{segment} {names}") from exc


def _reproduce(
    stages: List["Stage"],
    graph: Optional["DiGraph"] = None,
    force_downstream: bool = False,
    on_error: str = "fail",
    force: bool = False,
    repro_fn: Callable = _reproduce_stage,
    **kwargs,
) -> List["Stage"]:
    assert on_error in ("fail", "keep-going", "ignore")

    result: List["Stage"] = []
    failed: List["Stage"] = []
    to_skip: Dict["Stage", "Stage"] = {}
    ret: Optional["Stage"] = None

    force_state = {node: force for node in stages}

    for stage in stages:
        if stage in to_skip:
            continue

        if ret:
            logger.info("")  # add a newline

        upstream, downstream = _get_upstream_downstream_nodes(graph, stage)
        force_stage = force_state[stage]

        try:
            ret = repro_fn(stage, upstream=upstream, force=force_stage, **kwargs)
        except Exception as exc:  # noqa: BLE001
            failed.append(stage)
            if on_error == "fail":
                _raise_error(exc, stage)

            dependents = handle_error(graph, on_error, exc, stage)
            to_skip.update({node: stage for node in dependents})
            continue

        if force_downstream and (ret or force_stage):
            force_state.update({node: True for node in downstream})

        if ret:
            result.append(ret)

    if on_error != "ignore" and failed:
        _raise_error(None, *failed)
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
    on_error: Optional[str] = "fail",
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

    graph = None
    steps = stages
    if not single_item:
        graph = get_active_graph(self.index.graph)
        steps = plan_repro(graph, stages, pipeline=pipeline, downstream=downstream)
    return _reproduce(steps, graph=graph, on_error=on_error or "fail", **kwargs)
