import logging
import typing
from functools import partial

from dvc.exceptions import ReproductionError
from dvc.repo.scm_context import scm_context
from dvc.stage.run import CheckpointKilledError

from . import locked
from .graph import get_pipeline, get_pipelines

if typing.TYPE_CHECKING:
    from . import Repo

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


@locked
@scm_context
def reproduce(
    self: "Repo",
    targets=None,
    recursive=False,
    pipeline=False,
    all_pipelines=False,
    **kwargs,
):
    glob = kwargs.pop("glob", False)
    accept_group = not glob

    if isinstance(targets, str):
        targets = [targets]

    if not all_pipelines and not targets:
        from dvc.dvcfile import PIPELINE_FILE

        targets = [PIPELINE_FILE]

    interactive = kwargs.get("interactive", False)
    if not interactive:
        kwargs["interactive"] = self.config["core"].get("interactive", False)

    stages = set()
    if pipeline or all_pipelines:
        pipelines = get_pipelines(self.graph)
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
                    accept_group=accept_group,
                    glob=glob,
                )
            )

    return _reproduce_stages(self.graph, list(stages), **kwargs)


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
    steps = _get_steps(G, stages, downstream, single_item)

    force_downstream = kwargs.pop("force_downstream", False)
    result = []
    unchanged = []
    # `ret` is used to add a cosmetic newline.
    ret = []
    checkpoint_func = kwargs.pop("checkpoint_func", None)
    for stage in steps:
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


def _get_steps(G, stages, downstream, single_item):
    import networkx as nx

    active = G.copy()
    if not single_item:
        # NOTE: frozen stages don't matter for single_item
        for stage in G:
            if stage.frozen:
                # NOTE: disconnect frozen stage from its dependencies
                active.remove_edges_from(G.out_edges(stage))

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
