from __future__ import unicode_literals

import os

import dvc.logger as logger
from dvc.exceptions import ReproductionError
from dvc.repo.scm_context import scm_context


def _reproduce_stage(stages, node, force, dry, interactive, no_commit):
    stage = stages[node]

    if stage.locked:
        logger.warning(
            "DVC file '{path}' is locked. Its dependencies are"
            " not going to be reproduced.".format(path=stage.relpath)
        )

    stage = stage.reproduce(
        force=force, dry=dry, interactive=interactive, no_commit=no_commit
    )
    if not stage:
        return []

    if not dry:
        stage.dump()

    return [stage]


@scm_context
def reproduce(
    self,
    target=None,
    recursive=True,
    force=False,
    dry=False,
    interactive=False,
    pipeline=False,
    all_pipelines=False,
    ignore_build_cache=False,
    no_commit=False,
):
    from dvc.stage import Stage

    if not target and not all_pipelines:
        raise ValueError()

    if not interactive:
        config = self.config
        core = config.config[config.SECTION_CORE]
        interactive = core.get(config.SECTION_CORE_INTERACTIVE, False)

    targets = []
    if pipeline or all_pipelines:
        if pipeline:
            stage = Stage.load(self, target)
            node = os.path.relpath(stage.path, self.root_dir)
            pipelines = [self._get_pipeline(node)]
        else:
            pipelines = self.pipelines()

        for G in pipelines:
            for node in G.nodes():
                if G.in_degree(node) == 0:
                    targets.append(os.path.join(self.root_dir, node))
    else:
        targets.append(target)

    ret = []
    with self.state:
        for target in targets:
            stages = _reproduce(
                self,
                target,
                recursive=recursive,
                force=force,
                dry=dry,
                interactive=interactive,
                ignore_build_cache=ignore_build_cache,
                no_commit=no_commit,
            )
            ret.extend(stages)

    return ret


def _reproduce(
    self,
    target,
    recursive=True,
    force=False,
    dry=False,
    interactive=False,
    ignore_build_cache=False,
    no_commit=False,
):
    import networkx as nx
    from dvc.stage import Stage

    stage = Stage.load(self, target)
    G = self.graph()[1]
    stages = nx.get_node_attributes(G, "stage")
    node = os.path.relpath(stage.path, self.root_dir)

    if recursive:
        ret = _reproduce_stages(
            G,
            stages,
            node,
            force,
            dry,
            interactive,
            ignore_build_cache,
            no_commit,
        )
    else:
        ret = _reproduce_stage(
            stages, node, force, dry, interactive, no_commit
        )

    return ret


def _reproduce_stages(
    G, stages, node, force, dry, interactive, ignore_build_cache, no_commit
):
    import networkx as nx

    result = []
    for n in nx.dfs_postorder_nodes(G, node):
        try:
            ret = _reproduce_stage(
                stages, n, force, dry, interactive, no_commit
            )

            if len(ret) != 0 and ignore_build_cache:
                # NOTE: we are walking our pipeline from the top to the
                # bottom. If one stage is changed, it will be reproduced,
                # which tells us that we should force reproducing all of
                # the other stages down below, even if their direct
                # dependencies didn't change.
                force = True

            result += ret
        except Exception as ex:
            raise ReproductionError(stages[n].relpath, ex)
    return result
