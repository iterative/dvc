import argparse
from typing import TYPE_CHECKING, Hashable, Optional

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

if TYPE_CHECKING:
    from networkx import DiGraph

    from dvc.repo import Repo
    from dvc.stage import Stage

VALIDATION_ATTRIBUTES = [
    "outs clean",
    "outs pushed",
    "deps clean",
    "deps pushed",
    "validated",
    "command run",
]


def _show_ascii(graph: "DiGraph"):
    from dvc.dagascii import draw
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(graph)

    ret = []
    for pipeline in pipelines:
        ret.append(draw(pipeline.nodes, pipeline.edges))

    return "\n".join(ret)


def _quote_label(node):
    label = str(node)
    # Node names should not contain ":" unless they are quoted with "".
    # See: https://github.com/pydot/pydot/issues/258.
    if label[0] != '"' and label[-1] != '"':
        return f'"{label}"'
    return label


def _show_dot(graph: "DiGraph"):
    import io

    import networkx as nx
    from networkx.drawing.nx_pydot import write_dot

    dot_file = io.StringIO()

    nx.relabel_nodes(graph, _quote_label, copy=False)
    write_dot(graph.reverse(), dot_file)
    return dot_file.getvalue()


def _show_mermaid(
    graph, markdown: bool = False, direction: str = "TD", status: bool = False
):
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(graph)

    output = f"flowchart {direction}"

    if status:
        output += _get_class_defs()

    total_nodes = 0
    for pipeline in pipelines:
        node_ids = {}
        nodes = sorted(str(x) for x in pipeline.nodes)
        for node in nodes:
            total_nodes += 1
            node_id = f"node{total_nodes}"
            node_str = f'\n\t{node_id}["{node}"]'
            if status:
                node_str += f":::{pipeline.nodes[node]['status']}"
            node_ids[node] = node_id
            output += node_str
        edges = sorted((str(a), str(b)) for b, a in pipeline.edges)
        for a, b in edges:
            output += f"\n\t{node_ids[str(a)]}-->{node_ids[str(b)]}"

    if markdown:
        return f"```mermaid\n{output}\n```"

    return output


def _get_class_defs() -> str:
    import textwrap

    classes_dict = {
        "red": "fill:#e74c3c,color:#fff",
        "orange": "fill:#f1c40f,color:#000",
        "green": "fill:#2ecc71,color:#000",
        "grey": "fill:#95a5a6,color:#000",
        "blue": "fill:#3498db,color:#fff",
        "blue_grey": "fill:#3498db,color:#fff,stroke:#2ecc71,stroke-width:8px",
        "blue_red": "fill:#3498db,color:#fff,stroke:#e74c3c,stroke-width:8px",
        "blue_orange": "fill:#3498db,color:#fff,stroke:#f1c40f,stroke-width:8px",
    }
    classes = [f"classDef {key} {val}" for key, val in classes_dict.items()]

    return textwrap.indent("\n".join(classes), "\t")


def _collect_targets(repo, target, outs):
    if not target:
        return []

    pairs = repo.stage.collect_granular(target)
    if not outs:
        return [stage.addressing for stage, _ in pairs]

    targets = []

    outs_trie = repo.index.outs_trie
    for stage, path in pairs:
        if not path:
            targets.extend([str(out) for out in stage.outs])
            continue

        for out in outs_trie.itervalues(prefix=repo.fs.path.parts(path)):  # noqa: B301
            targets.extend(str(out))

    return targets


def _transform(index, outs):
    import networkx as nx

    from dvc.stage import Stage

    def _relabel(node) -> str:
        return node.addressing if isinstance(node, Stage) else str(node)

    graph = index.outs_graph if outs else index.graph
    return nx.relabel_nodes(graph, _relabel, copy=True)


def _filter(graph, targets, full):
    import networkx as nx

    if not targets:
        return graph

    new_graph = graph.copy()
    if not full:
        descendants = set()
        for target in targets:
            descendants.update(nx.descendants(graph, target))
            descendants.add(target)
        new_graph.remove_nodes_from(set(graph.nodes()) - descendants)

    undirected = new_graph.to_undirected()
    connected = set()
    for target in targets:
        connected.update(nx.node_connected_component(undirected, target))

    new_graph.remove_nodes_from(set(new_graph.nodes()) - connected)
    return new_graph


def _build(repo, target=None, full=False, outs=False):
    targets = _collect_targets(repo, target, outs)
    graph = _transform(repo.index, outs)
    return _filter(graph, targets, full)


def _set_stage_info(pipeline: "DiGraph", stage: "Stage", repo_status: dict) -> None:
    """
    Sets the 'command run', 'frozen' and 'is_import' attribute for the given
    stage node in the pipeline. 'command run' indicates whether a stage has
    run with the current command before.

    Args:
        pipeline: The pipeline graph containing the stage nodes.
        stage: The stage whose 'command run' attribute is to be set.
        repo_status: A dictionary containing the status of the repository.

    Returns:
        None.
    """
    pipeline.nodes[stage.addressing]["frozen"] = stage.frozen
    pipeline.nodes[stage.addressing]["import"] = stage.is_import

    if stage.addressing not in repo_status:
        pipeline.nodes[stage.addressing]["command run"] = True
        return

    pipeline.nodes[stage.addressing]["command run"] = (
        "changed command" not in repo_status[stage.addressing]
    )


def _set_stage_resource_info(
    pipeline: "DiGraph",
    stage: "Stage",
    cloud_status: dict,
    resource: str,
) -> None:
    """
    Updates the status attributes stages deps or outs.

    Args:
        pipeline: The pipeline graph containing the stage nodes.
        stage: The stage whose status attributes are to be updated.
        cloud_status: A dictionary containing the cloud status of the resources.
        resource: Whether to update the 'deps' or the 'outs' of the stage.

    Returns:
        None.

    Raises:
        ValueError: If the resource argument is not 'deps' or 'outs'.
    """

    if resource == "deps":
        rsc_list = stage.deps
        rsc_key = "deps"
    elif resource == "outs":
        rsc_list = stage.outs
        rsc_key = "outs"
    else:
        raise ValueError("resource must be either 'deps' or 'outs'")

    if rsc_list:
        pipeline.nodes[stage.addressing][rsc_key] = {}
        all_resources_clean = True
        all_resources_pushed = True
        for rsc in rsc_list:
            resource_name = str(rsc)
            resource_clean = True
            resource_pushed = True

            if rsc.status():
                resource_clean = False
                all_resources_clean = False

            if str(rsc) in cloud_status:
                resource_pushed = False
                all_resources_pushed = False

            pipeline.nodes[stage.addressing][rsc_key][resource_name] = {
                "clean": resource_clean,
                "pushed": resource_pushed,
            }

        pipeline.nodes[stage.addressing][rsc_key + " clean"] = all_resources_clean
        pipeline.nodes[stage.addressing][rsc_key + " pushed"] = all_resources_pushed


def _invalidate_downstream(pipeline: "DiGraph", node: Hashable) -> None:
    """
    Invalidates the downstream nodes of the given node in the pipeline
    by setting their 'validated' attribute to False.

    Args:
        pipeline: The pipeline graph containing the node.
        node: The node whose downstream nodes are to be invalidated.

    Returns:
        None.
    """
    # Don't invalidate frozen nodes
    if not pipeline.nodes[node]["frozen"]:
        # Set the 'validated' attribute to False for the current node
        pipeline.nodes[node]["validated"] = False

        # Recursively invalidate downstream nodes
        for successor_id in pipeline.successors(node):
            _invalidate_downstream(pipeline, successor_id)


def _validate_pipeline(pipeline: "DiGraph") -> None:
    """
    Validates the nodes in the pipeline by setting their 'validated'
    attribute based on whether they have changes or upstream changes.

    Args:
        pipeline: The pipeline graph containing the nodes.

    Returns:
        None.
    """
    # Set the 'validated' attribute to True for all nodes by default
    for node in pipeline.nodes:
        pipeline.nodes[node]["validated"] = True

    for node in pipeline.nodes:
        node_data = pipeline.nodes[node]

        # Check if any attribute has a False value
        if not all(node_data.get(key, True) for key in VALIDATION_ATTRIBUTES):
            _invalidate_downstream(pipeline, node)


def _set_stage_status(pipeline: "DiGraph"):
    """
    Sets the 'status' attribute for each stage node in the pipeline,
    summarizing its status for rendering in a DAG.

    Args:
        pipeline: The pipeline graph containing the stage nodes.

    Returns:
        None.
    """
    for _, data in pipeline.nodes(data=True):
        command_run = data.get("command run", None)
        outs_clean = data.get("outs clean", None)
        outs_pushed = data.get("outs pushed", None)
        deps_clean = data.get("deps clean", None)
        deps_pushed = data.get("deps pushed", None)
        validated = data.get("validated", None)
        frozen = data.get("frozen", False)

        # Stage is not validated but otherwise green -> grey
        if validated is False and all(
            [outs_clean, outs_pushed, deps_clean, deps_pushed, command_run]
        ):
            data["status"] = "grey"

        # Stage has any dirty outs, deps or command -> red
        elif outs_clean is False or deps_clean is False or command_run is False:
            data["status"] = "red"

        # Stage has any unpushed outs or deps -> orange
        elif outs_pushed is False or deps_pushed is False:
            data["status"] = "orange"

        # Stage has none of the above -> green
        else:
            data["status"] = "green"

        # Overwrite if stage is frozen, combine with previously set status
        if frozen:
            if data["status"] == "green":
                data["status"] = "blue"
            else:
                data["status"] = f"blue_{data['status']}"


def _update_stage_status(
    repo: "Repo", target: Optional[str], graph: "DiGraph", status_import: bool = False
) -> "DiGraph":
    """
    Updates all status attributes of stages in the graph.

    Args:
        repo: The repository object that contains the stages.
        graph: The pipeline graph containing the stage nodes.
        status_import: Whether to set resource information for import dependencies.

    Returns:
        The updated graph with all status attributes of stages updated.
    """
    repo_status = repo.status(targets=target)
    cloud_status = repo.status(targets=target, cloud=True)
    if status_import:
        pass

    if target:
        pass

    for stage in repo.index.stages:
        # ignore stages that are not in pipeline
        if stage.addressing in graph.nodes:
            _set_stage_info(graph, stage, repo_status)
            _set_stage_resource_info(graph, stage, cloud_status, "outs")

            if not stage.is_import or status_import:
                _set_stage_resource_info(graph, stage, cloud_status, "deps")

    _validate_pipeline(graph)
    _set_stage_status(graph)

    return graph


class CmdDAG(CmdBase):
    def run(self):
        graph = _build(
            self.repo,
            target=self.args.target,
            full=self.args.full,
            outs=self.args.outs,
        )

        if self.args.status and not self.args.dot:
            graph = _update_stage_status(
                self.repo, self.args.target, graph, self.args.status_import
            )

        if self.args.dot:
            ui.write(_show_dot(graph))
        elif self.args.mermaid or self.args.markdown or self.args.status:
            ui.write(
                _show_mermaid(
                    graph, self.args.markdown, self.args.direction, self.args.status
                )
            )
        else:
            with ui.pager():
                ui.write(_show_ascii(graph))

        return 0


def add_parser(subparsers, parent_parser):
    DAG_HELP = "Visualize DVC project DAG."
    dag_parser = subparsers.add_parser(
        "dag",
        parents=[parent_parser],
        description=append_doc_link(DAG_HELP, "dag"),
        help=DAG_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    dag_parser.add_argument(
        "--dot",
        action="store_true",
        default=False,
        help="Print DAG with .dot format.",
    )
    dag_parser.add_argument(
        "--mermaid",
        action="store_true",
        default=False,
        help="Print DAG with mermaid format.",
    )
    dag_parser.add_argument(
        "--md",
        "--show-md",
        action="store_true",
        default=False,
        dest="markdown",
        help="Print DAG with mermaid format wrapped in Markdown block.",
    )
    dag_parser.add_argument(
        "--full",
        action="store_true",
        default=False,
        help=(
            "Show full DAG that the target belongs too, instead of "
            "showing DAG consisting only of ancestors."
        ),
    )
    dag_parser.add_argument(
        "-o",
        "--outs",
        action="store_true",
        default=False,
        help="Print output files instead of stages.",
    )
    dag_parser.add_argument(
        "--direction",
        choices=["LR", "TD"],
        default="TD",
        help=(
            "Direction of the rendered mermaid DAG. "
            "Can either be 'LR' for left-to-right or 'TD' for top-down'."
        ),
    )
    dag_parser.add_argument(
        "--status",
        action="store_true",
        default=False,
        help=(
            "Check the status of stages in the DAG. "
            "(Only compatible with --mermaid and --md)"
        ),
    )
    dag_parser.add_argument(
        "--status-import",
        action="store_true",
        default=False,
        help="Check the dependencies of import stages. (Only compatible with --status)",
    )
    dag_parser.add_argument(
        "target",
        nargs="?",
        help=(
            "Stage name or output to show pipeline for. "
            "Finds all stages in the workspace by default."
        ),
    )
    dag_parser.set_defaults(func=CmdDAG)
