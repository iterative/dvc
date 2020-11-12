import logging
from functools import partial

from dvc.exceptions import InvalidArgumentError, ReproductionError
from dvc.repo.scm_context import scm_context
from dvc.stage.run import CheckpointKilledError

from . import locked
from .graph import get_pipeline, get_pipelines

logger = logging.getLogger(__name__)


def _reproduce_stage(stage, **kwargs):
    def _run_callback(repro_callback):
        _dump_stage(stage)
        repro_callback([stage])

    checkpoint_func = kwargs.pop("checkpoint_func", None)
    if stage.is_checkpoint:
        if checkpoint_func:
            kwargs["checkpoint_func"] = partial(_run_callback, checkpoint_func)
        else:
            logger.warning(
                "Checkpoint stages are not fully supported in 'dvc repro'. "
                "Checkpoint stages should be reproduced with 'dvc exp run' "
                "or 'dvc exp resume'."
            )

    if stage.frozen and not stage.is_import:
        logger.warning(
            "{} is frozen. Its dependencies are"
            " not going to be reproduced.".format(stage)
        )

    stage = stage.reproduce(**kwargs)
    if not stage:
        return []

    if not kwargs.get("dry", False):
        _dump_stage(stage)

    return [stage]


def _dump_stage(stage):
    from ..dvcfile import Dvcfile

    dvcfile = Dvcfile(stage.repo, stage.path)
    dvcfile.dump(stage, update_pipeline=False)


def _get_active_graph(G):
    import networkx as nx

    active = G.copy()
    for stage in G:
        if not stage.frozen:
            continue
        active.remove_edges_from(G.out_edges(stage))
        for edge in G.out_edges(stage):
            _, to_stage = edge
            for node in nx.dfs_preorder_nodes(G, to_stage):
                # NOTE: `in_degree` will return InDegreeView({}) if stage
                # no longer exists in the `active` DAG.
                if not active.in_degree(node):
                    # NOTE: if some edge no longer exists `remove_edges_from`
                    # will ignore it without error.
                    active.remove_edges_from(G.out_edges(node))
                    active.remove_node(node)

    return active


@locked
@scm_context
def reproduce(
    self,
    target=None,
    recursive=False,
    pipeline=False,
    all_pipelines=False,
    **kwargs,
):
    from dvc.utils import parse_target

    assert target is None or isinstance(target, str)
    if not target and not all_pipelines:
        raise InvalidArgumentError(
            "Neither `target` nor `--all-pipelines` are specified."
        )

    interactive = kwargs.get("interactive", False)
    if not interactive:
        kwargs["interactive"] = self.config["core"].get("interactive", False)

    active_graph = _get_active_graph(self.graph)
    active_pipelines = get_pipelines(active_graph)

    path, name = parse_target(target)
    if pipeline or all_pipelines:
        if all_pipelines:
            pipelines = active_pipelines
        else:
            stage = self.get_stage(path, name)
            pipelines = [get_pipeline(active_pipelines, stage)]

        targets = []
        for pipeline in pipelines:
            for stage in pipeline:
                if pipeline.in_degree(stage) == 0:
                    targets.append(stage)
    else:
        targets = self.collect(target, recursive=recursive, graph=active_graph)

    return _reproduce_stages(active_graph, targets, **kwargs)


def _reproduce_stages(
    G, stages, downstream=False, single_item=False, on_unchanged=None, **kwargs
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
    pipeline = _get_pipeline(G, stages, downstream, single_item)

    force_downstream = kwargs.pop("force_downstream", False)
    result = []
    unchanged = []
    # `ret` is used to add a cosmetic newline.
    ret = []
    checkpoint_func = kwargs.pop("checkpoint_func", None)
    for stage in pipeline:
        if ret:
            logger.info("")

        if checkpoint_func:
            kwargs["checkpoint_func"] = partial(
                _repro_callback, checkpoint_func, unchanged
            )

        try:
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
        except CheckpointKilledError:
            raise
        except Exception as exc:
            raise ReproductionError(stage.relpath) from exc

    if on_unchanged is not None:
        on_unchanged(unchanged)
    return result


def _get_pipeline(G, stages, downstream, single_item):
    import networkx as nx

    if single_item:
        all_pipelines = stages
    else:
        all_pipelines = []
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
                nodes = nx.dfs_postorder_nodes(
                    G.copy().reverse(copy=False), stage
                )
                all_pipelines += reversed(list(nodes))
            else:
                all_pipelines += nx.dfs_postorder_nodes(G, stage)

    pipeline = []
    for stage in all_pipelines:
        if stage not in pipeline:
            pipeline.append(stage)

    return pipeline


def _repro_callback(experiments_callback, unchanged, stages):
    experiments_callback(unchanged, stages)
