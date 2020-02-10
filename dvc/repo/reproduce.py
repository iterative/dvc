import logging

from dvc.exceptions import ReproductionError
from dvc.repo.scm_context import scm_context
from . import locked
from .graph import get_pipeline, get_pipelines


logger = logging.getLogger(__name__)


def _reproduce_stage(stage, **kwargs):
    if stage.locked:
        logger.warning(
            "DVC-file '{path}' is locked. Its dependencies are"
            " not going to be reproduced.".format(path=stage.relpath)
        )

    stage = stage.reproduce(**kwargs)
    if not stage:
        return []

    if not kwargs.get("dry", False):
        stage.dump()

    return [stage]


def _get_active_graph(G):
    import networkx as nx

    active = G.copy()
    for stage in G:
        if not stage.locked:
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
    **kwargs
):
    from dvc.stage import Stage

    if not target and not all_pipelines:
        raise ValueError()

    interactive = kwargs.get("interactive", False)
    if not interactive:
        config = self.config
        core = config.config[config.SECTION_CORE]
        kwargs["interactive"] = core.get(
            config.SECTION_CORE_INTERACTIVE, False
        )

    active_graph = _get_active_graph(self.graph)
    active_pipelines = get_pipelines(active_graph)

    if pipeline or all_pipelines:
        if all_pipelines:
            pipelines = active_pipelines
        else:
            stage = Stage.load(self, target)
            pipelines = [get_pipeline(active_pipelines, stage)]

        targets = []
        for pipeline in pipelines:
            for stage in pipeline:
                if pipeline.in_degree(stage) == 0:
                    targets.append(stage)
    else:
        targets = self.collect(target, recursive=recursive, graph=active_graph)

    ret = []
    for target in targets:
        stages = _reproduce_stages(active_graph, target, **kwargs)
        ret.extend(stages)

    return ret


def _reproduce_stages(
    G,
    stage,
    downstream=False,
    ignore_build_cache=False,
    single_item=False,
    **kwargs
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
    use a pre-ordered search using the given stage as a starting point.

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

    if single_item:
        pipeline = [stage]
    elif downstream:
        # NOTE (py3 only):
        # Python's `deepcopy` defaults to pickle/unpickle the object.
        # Stages are complex objects (with references to `repo`, `outs`,
        # and `deps`) that cause struggles when you try to serialize them.
        # We need to create a copy of the graph itself, and then reverse it,
        # instead of using graph.reverse() directly because it calls
        # `deepcopy` underneath -- unless copy=False is specified.
        pipeline = nx.dfs_preorder_nodes(G.copy().reverse(copy=False), stage)
    else:
        pipeline = nx.dfs_postorder_nodes(G, stage)

    result = []
    for st in pipeline:
        try:
            ret = _reproduce_stage(st, **kwargs)

            if len(ret) != 0 and ignore_build_cache:
                # NOTE: we are walking our pipeline from the top to the
                # bottom. If one stage is changed, it will be reproduced,
                # which tells us that we should force reproducing all of
                # the other stages down below, even if their direct
                # dependencies didn't change.
                kwargs["force"] = True

            result.extend(ret)
        except Exception as exc:
            raise ReproductionError(st.relpath) from exc
    return result
