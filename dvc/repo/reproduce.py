import logging
from typing import TYPE_CHECKING, Iterator, List

from dvc.exceptions import ReproductionError
from dvc.repo.scm_context import scm_context
from dvc.stage.cache import RunCacheNotSupported

from . import locked

if TYPE_CHECKING:
    from dvc.stage import Stage

    from . import Repo

logger = logging.getLogger(__name__)


def _reproduce_stage(stage: "Stage", **kwargs) -> List["Stage"]:
    if stage.frozen and not stage.is_import:
        logger.warning(
            "%s is frozen. Its dependencies are not going to be reproduced.",
            stage,
        )

    stage = stage.reproduce(**kwargs)
    if not stage:
        return []

    if not kwargs.get("dry", False):
        stage.dump(update_pipeline=False)
        _track_stage(stage)

    return [stage]


def _get_stage_files(stage: "Stage") -> Iterator[str]:
    yield stage.dvcfile.relpath
    for dep in stage.deps:
        if (
            not dep.use_scm_ignore
            and dep.is_in_repo
            and not stage.repo.dvcfs.isdvc(stage.repo.dvcfs.from_os_path(str(dep)))
        ):
            yield dep.fs_path
    for out in stage.outs:
        if not out.use_scm_ignore and out.is_in_repo:
            yield out.fs_path


def _track_stage(stage: "Stage") -> None:
    from dvc.utils import relpath

    context = stage.repo.scm_context
    for path in _get_stage_files(stage):
        context.track_file(relpath(path))
    return context.track_changed_files()


@locked
@scm_context
def reproduce(  # noqa: C901, PLR0912
    self: "Repo",
    targets=None,
    recursive=False,
    pipeline=False,
    all_pipelines=False,
    **kwargs,
):
    from .graph import get_pipeline, get_pipelines

    glob = kwargs.pop("glob", False)

    if isinstance(targets, str):
        targets = [targets]

    if not all_pipelines and not targets:
        from dvc.dvcfile import PROJECT_FILE

        targets = [PROJECT_FILE]

    interactive = kwargs.get("interactive", False)
    if not interactive:
        kwargs["interactive"] = self.config["core"].get("interactive", False)

    stages = set()
    if pipeline or all_pipelines:
        pipelines = get_pipelines(self.index.graph)
        if all_pipelines:
            used_pipelines = pipelines
        else:
            used_pipelines = []
            for target in targets:
                stage = self.stage.get_target(target)
                used_pipelines.append(get_pipeline(pipelines, stage))

        for pline in used_pipelines:
            for stage in pline:
                if pline.in_degree(stage) == 0:
                    stages.add(stage)
    else:
        for target in targets:
            stages.update(
                self.stage.collect(
                    target,
                    recursive=recursive,
                    glob=glob,
                )
            )

    if kwargs.get("pull", False) and kwargs.get("run_cache", True):
        logger.debug("Pulling run cache")
        try:
            self.stage_cache.pull(None)
        except RunCacheNotSupported as e:
            logger.warning("Failed to pull run cache: %s", e)

    return _reproduce_stages(self.index.graph, list(stages), **kwargs)


def _reproduce_stages(  # noqa: C901
    graph,
    stages,
    downstream=False,
    single_item=False,
    on_unchanged=None,
    **kwargs,
):
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
    steps = _get_steps(graph, stages, downstream, single_item)

    force_downstream = kwargs.pop("force_downstream", False)
    result: List["Stage"] = []
    unchanged: List["Stage"] = []
    # `ret` is used to add a cosmetic newline.
    ret: List["Stage"] = []

    for stage in steps:
        if ret:
            logger.info("")

        try:
            kwargs["upstream"] = result + unchanged
            ret = _reproduce_stage(stage, **kwargs)

            if len(ret) == 0:
                unchanged.extend([stage])
            elif force_downstream:
                # NOTE: we are walking our pipeline from the top to the
                # bottom. If one stage is changed, it will be reproduced,
                # which tells us that we should force reproducing all of
                # the other stages down below, even if their direct
                # dependencies didn't change.
                kwargs["force"] = True

            if ret:
                result.extend(ret)
        except Exception as exc:  # noqa: BLE001
            raise ReproductionError(stage.addressing) from exc

    if on_unchanged is not None:
        on_unchanged(unchanged)
    return result


def _get_steps(graph, stages, downstream, single_item):
    import networkx as nx

    active = graph.copy()
    if not single_item:
        # NOTE: frozen stages don't matter for single_item
        for stage in graph:
            if stage.frozen:
                # NOTE: disconnect frozen stage from its dependencies
                active.remove_edges_from(graph.out_edges(stage))

    all_pipelines: List["Stage"] = []
    for stage in stages:
        if downstream:
            # NOTE (py3 only):
            # Python's `deepcopy` defaults to pickle/unpickle the object.
            # Stages are complex objects (with references to `repo`,
            # `outs`, and `deps`) that cause struggles when you try
            # to serialize them. We need to create a copy of the graph
            # itself, and then reverse it, instead of using
            # graph.reverse() directly because it calls `deepcopy`
            # underneath -- unless copy=False is specified.
            nodes = nx.dfs_postorder_nodes(active.reverse(copy=False), stage)
            all_pipelines += reversed(list(nodes))
        else:
            all_pipelines += nx.dfs_postorder_nodes(active, stage)

    steps = []
    for stage in all_pipelines:
        if stage not in steps:
            # NOTE: order of steps still matters for single_item
            if single_item and stage not in stages:
                continue

            steps.append(stage)

    return steps


def _repro_callback(experiments_callback, unchanged, stages):
    experiments_callback(unchanged, stages)
