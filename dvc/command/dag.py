import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link
from dvc.exceptions import DvcException

logger = logging.getLogger(__name__)


def _show_ascii(G):
    from dvc.dagascii import draw
    from dvc.repo.graph import get_pipelines

    pipelines = get_pipelines(G)

    ret = []
    for pipeline in pipelines:
        ret.append(draw(pipeline.nodes, pipeline.edges))

    return "\n".join(ret)


def _show_dot(G):
    import io

    from networkx.drawing.nx_pydot import write_dot

    dot_file = io.StringIO()
    write_dot(G, dot_file)
    return dot_file.getvalue()


def _build(G, target=None, full=False):
    import networkx as nx

    from dvc.repo.graph import get_pipeline, get_pipelines

    if target:
        H = get_pipeline(get_pipelines(G), target)
        if not full:
            descendants = nx.descendants(G, target)
            descendants.add(target)
            H.remove_nodes_from(set(G.nodes()) - descendants)
    else:
        H = G

    def _relabel(stage):
        return stage.addressing

    return nx.relabel_nodes(H, _relabel, copy=False)


class CmdDAG(CmdBase):
    def run(self):
        try:
            target = None
            if self.args.target:
                stages = self.repo.collect(self.args.target)
                if len(stages) > 1:
                    logger.error(
                        f"'{self.args.target}' contains more than one stage "
                        "{stages}, please specify one stage"
                    )
                    return 1
                target = stages[0]

            G = _build(self.repo.graph, target=target, full=self.args.full,)

            if self.args.dot:
                logger.info(_show_dot(G))
            else:
                from dvc.utils.pager import pager

                pager(_show_ascii(G))

            return 0
        except DvcException:
            msg = "failed to show "
            if self.args.target:
                msg += f"a pipeline for '{target}'"
            else:
                msg += "pipelines"
            logger.exception(msg)
            return 1


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
        "--full",
        action="store_true",
        default=False,
        help=(
            "Show full DAG that the target belongs too, instead of "
            "showing DAG consisting only of ancestors."
        ),
    )
    dag_parser.add_argument(
        "target",
        nargs="?",
        help="Stage or output to show pipeline for (optional). "
        "Finds all stages in the workspace by default.",
    )
    dag_parser.set_defaults(func=CmdDAG)
