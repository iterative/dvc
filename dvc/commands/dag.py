import argparse
from typing import TYPE_CHECKING

from dvc.cli.command import CmdBase
from dvc.cli.utils import append_doc_link
from dvc.ui import ui

if TYPE_CHECKING:
    from networkx import DiGraph


def _show_ascii(G: "DiGraph"):
    from dvc.dagascii import draw
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(G)

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


def _show_dot(G: "DiGraph"):
    import io

    import networkx as nx
    from networkx.drawing.nx_pydot import write_dot

    dot_file = io.StringIO()

    nx.relabel_nodes(G, _quote_label, copy=False)
    write_dot(G.reverse(), dot_file)
    return dot_file.getvalue()


def _show_mermaid(G, markdown: bool = False):
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(G)

    graph = "flowchart TD"

    total_nodes = 0
    for pipeline in pipelines:
        node_ids = {}
        nodes = sorted(str(x) for x in pipeline.nodes)
        for node in nodes:
            total_nodes += 1
            node_id = f"node{total_nodes}"
            graph += f'\n\t{node_id}["{node}"]'
            node_ids[node] = node_id
        edges = sorted((str(a), str(b)) for b, a in pipeline.edges)
        for a, b in edges:
            graph += f"\n\t{node_ids[str(a)]}-->{node_ids[str(b)]}"

    if markdown:
        return f"```mermaid\n{graph}\n```"

    return graph


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

        for out in outs_trie.itervalues(  # noqa: B301
            prefix=repo.fs.path.parts(path)
        ):
            targets.extend(str(out))

    return targets


def _transform(index, outs):
    import networkx as nx

    from dvc.stage import Stage

    def _relabel(node) -> str:
        return node.addressing if isinstance(node, Stage) else str(node)

    G = index.outs_graph if outs else index.graph
    return nx.relabel_nodes(G, _relabel, copy=True)


def _filter(G, targets, full):
    import networkx as nx

    if not targets:
        return G

    H = G.copy()
    if not full:
        descendants = set()
        for target in targets:
            descendants.update(nx.descendants(G, target))
            descendants.add(target)
        H.remove_nodes_from(set(G.nodes()) - descendants)

    undirected = H.to_undirected()
    connected = set()
    for target in targets:
        connected.update(nx.node_connected_component(undirected, target))

    H.remove_nodes_from(set(H.nodes()) - connected)

    return H


def _build(repo, target=None, full=False, outs=False):
    targets = _collect_targets(repo, target, outs)
    G = _transform(repo.index, outs)
    return _filter(G, targets, full)


class CmdDAG(CmdBase):
    def run(self):
        G = _build(
            self.repo,
            target=self.args.target,
            full=self.args.full,
            outs=self.args.outs,
        )

        if self.args.dot:
            ui.write(_show_dot(G))
        elif self.args.mermaid or self.args.markdown:
            ui.write(_show_mermaid(G, self.args.markdown))
        else:
            with ui.pager():
                ui.write(_show_ascii(G))

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
        "target",
        nargs="?",
        help="Stage or output to show pipeline for (optional). "
        "Finds all stages in the workspace by default.",
    )
    dag_parser.set_defaults(func=CmdDAG)
