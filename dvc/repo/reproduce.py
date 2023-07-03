import logging
from typing import TYPE_CHECKING, Iterable, List, Optional, Union, cast

from funcy import ldistinct

from dvc.exceptions import ReproductionError
from dvc.repo.scm_context import scm_context
from dvc.stage.cache import RunCacheNotSupported
from dvc.utils.collections import ensure_list

from . import locked

if TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.stage import Stage

    from . import Repo

logger = logging.getLogger(__name__)


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


def _remove_frozen_stages(graph: "DiGraph") -> "DiGraph":
    g = cast("DiGraph", graph.copy())
    for stage in graph:
        if stage.frozen:
            # NOTE: disconnect frozen stage from its dependencies
            g.remove_edges_from(graph.out_edges(stage))
    return g


def plan_repro(
    graph: "DiGraph",
    stages: Optional[List["Stage"]] = None,
    pipeline: bool = False,
    downstream: bool = False,
    all_pipelines: bool = False,
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
    from .graph import get_pipeline, get_pipelines, get_steps

    if pipeline or all_pipelines:
        pipelines = get_pipelines(graph)
        if stages and pipeline:
            pipelines = [get_pipeline(pipelines, stage) for stage in stages]

        leaves: List["Stage"] = []
        for pline in pipelines:
            leaves.extend(node for node in pline if pline.in_degree(node) == 0)
        stages = ldistinct(leaves)

    active = _remove_frozen_stages(graph)
    return get_steps(active, stages, downstream=downstream)


@locked
@scm_context
def reproduce(  # noqa: C901
    self: "Repo",
    targets: Union[Iterable[str], str, None] = None,
    recursive: bool = False,
    pipeline: bool = False,
    all_pipelines: bool = False,
    downstream: bool = False,
    single_item: bool = False,
    glob: bool = False,
    **kwargs,
):
    from dvc.dvcfile import PROJECT_FILE

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

    steps = stages
    if pipeline or all_pipelines or not single_item:
        graph = self.index.graph
        steps = plan_repro(
            graph,
            stages,
            pipeline=pipeline,
            downstream=downstream,
            all_pipelines=all_pipelines,
        )
    return _reproduce_stages(steps, **kwargs)


def _reproduce_stages(
    stages: List["Stage"],
    force_downstream: bool = False,
    **kwargs,
) -> List["Stage"]:
    result: List["Stage"] = []
    for i, stage in enumerate(stages):
        try:
            ret = _reproduce_stage(stage, upstream=stages[:i], **kwargs)
        except Exception as exc:  # noqa: BLE001
            raise ReproductionError(stage.addressing) from exc

        if not ret:
            continue

        result.append(ret)
        if force_downstream:
            # NOTE: we are walking our pipeline from the top to the
            # bottom. If one stage is changed, it will be reproduced,
            # which tells us that we should force reproducing all of
            # the other stages down below, even if their direct
            # dependencies didn't change.
            kwargs["force"] = True
        if i < len(stages) - 1:
            logger.info("")  # add a newline
    return result
