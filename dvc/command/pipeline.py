import argparse
import logging

from dvc.command.base import CmdBase, append_doc_link, fix_subparsers
from dvc.exceptions import DvcException


logger = logging.getLogger(__name__)


class CmdPipelineShow(CmdBase):
    def _show(self, target, commands, outs, locked):
        import networkx
        from dvc.stage import Stage

        stage = Stage.load(self.repo, target)
        G = self.repo.graph
        stages = networkx.dfs_postorder_nodes(G, stage)

        if locked:
            stages = [s for s in stages if s.locked]

        for stage in stages:
            if commands:
                if stage.cmd is None:
                    continue
                logger.info(stage.cmd)
            elif outs:
                for out in stage.outs:
                    logger.info(str(out))
            else:
                logger.info(stage.path_in_repo)

    def _build_graph(self, target, commands, outs):
        import networkx
        from dvc.stage import Stage
        from dvc.repo.graph import get_pipeline

        target_stage = Stage.load(self.repo, target)
        G = get_pipeline(self.repo.pipelines, target_stage)

        nodes = set()
        for stage in networkx.dfs_preorder_nodes(G, target_stage):
            if commands:
                if stage.cmd is None:
                    continue
                nodes.add(stage.cmd)
            elif outs:
                for out in stage.outs:
                    nodes.add(str(out))
                for dep in stage.deps:
                    nodes.add(str(dep))
            else:
                nodes.add(stage.relpath)

        edges = []

        if outs:
            for stage in networkx.dfs_preorder_nodes(G, target_stage):
                for dep in stage.deps:
                    for out in stage.outs:
                        edges.append((str(out), str(dep)))
        else:
            for from_stage, to_stage in networkx.dfs_edges(G, target_stage):
                if commands:
                    if to_stage.cmd is None:
                        continue
                    edges.append((from_stage.cmd, to_stage.cmd))
                else:
                    edges.append((from_stage.relpath, to_stage.relpath))

        return list(nodes), edges, networkx.is_tree(G)

    def _show_ascii(self, target, commands, outs):
        from dvc.dagascii import draw

        nodes, edges, _ = self._build_graph(target, commands, outs)

        if not nodes:
            return

        draw(nodes, edges)

    def _show_dependencies_tree(self, target, commands, outs):
        from treelib import Tree

        nodes, edges, is_tree = self._build_graph(target, commands, outs)
        if not nodes:
            return
        if not is_tree:
            raise DvcException(
                "DAG is not a tree, can not print it in tree-structure way, "
                "please use --ascii instead"
            )

        tree = Tree()
        tree.create_node(target, target)  # Root node
        observe_list = [target]
        while len(observe_list) > 0:
            current_root = observe_list[0]
            for edge in edges:
                if edge[0] == current_root:
                    tree.create_node(edge[1], edge[1], parent=current_root)
                    observe_list.append(edge[1])
            observe_list.pop(0)
        tree.show()

    def _write_dot(self, target, commands, outs):
        import io
        import networkx
        from networkx.drawing.nx_pydot import write_dot

        _, edges, _ = self._build_graph(target, commands, outs)
        edges = [edge[::-1] for edge in edges]

        simple_g = networkx.DiGraph()
        simple_g.add_edges_from(edges)

        dot_file = io.StringIO()
        write_dot(simple_g, dot_file)
        logger.info(dot_file.getvalue())

    def run(self):
        if not self.args.targets:
            self.args.targets = self.default_targets

        for target in self.args.targets:
            try:
                if self.args.ascii:
                    self._show_ascii(
                        target, self.args.commands, self.args.outs
                    )
                elif self.args.dot:
                    self._write_dot(target, self.args.commands, self.args.outs)
                elif self.args.tree:
                    self._show_dependencies_tree(
                        target, self.args.commands, self.args.outs
                    )
                else:
                    self._show(
                        target,
                        self.args.commands,
                        self.args.outs,
                        self.args.locked,
                    )
            except DvcException:
                msg = "failed to show pipeline for '{}'".format(target)
                logger.exception(msg)
                return 1
        return 0


class CmdPipelineList(CmdBase):
    def run(self):
        pipelines = self.repo.pipelines
        for pipeline in pipelines:
            for stage in pipeline:
                logger.info(stage.relpath)
            if len(pipeline) != 0:
                logger.info("=" * 80)
        logger.info("{} pipelines total".format(len(pipelines)))

        return 0


def add_parser(subparsers, parent_parser):
    PIPELINE_HELP = "Manage pipelines."
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        parents=[parent_parser],
        description=append_doc_link(PIPELINE_HELP, "pipeline"),
        help=PIPELINE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    pipeline_subparsers = pipeline_parser.add_subparsers(
        dest="cmd",
        help="Use `dvc pipeline CMD --help` for command-specific help.",
    )

    fix_subparsers(pipeline_subparsers)

    PIPELINE_SHOW_HELP = "Show pipelines."
    pipeline_show_parser = pipeline_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=append_doc_link(PIPELINE_SHOW_HELP, "pipeline/show"),
        help=PIPELINE_SHOW_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pipeline_show_group = pipeline_show_parser.add_mutually_exclusive_group()
    pipeline_show_group.add_argument(
        "-c",
        "--commands",
        action="store_true",
        default=False,
        help="Print commands instead of paths to DVC-files.",
    )
    pipeline_show_group.add_argument(
        "-o",
        "--outs",
        action="store_true",
        default=False,
        help="Print output files instead of paths to DVC-files.",
    )
    pipeline_show_parser.add_argument(
        "-l",
        "--locked",
        action="store_true",
        default=False,
        help="Print locked DVC stages",
    )
    pipeline_show_parser.add_argument(
        "--ascii",
        action="store_true",
        default=False,
        help="Output DAG as ASCII.",
    )
    pipeline_show_parser.add_argument(
        "--dot",
        action="store_true",
        default=False,
        help="Print DAG with .dot format.",
    )
    pipeline_show_parser.add_argument(
        "--tree",
        action="store_true",
        default=False,
        help="Output DAG as Dependencies Tree.",
    )
    pipeline_show_parser.add_argument(
        "targets",
        nargs="*",
        help="DVC-files to show pipeline for. Optional. "
        "(Finds all DVC-files in the workspace by default.)",
    )
    pipeline_show_parser.set_defaults(func=CmdPipelineShow)

    PIPELINE_LIST_HELP = "List pipelines."
    pipeline_list_parser = pipeline_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=append_doc_link(PIPELINE_LIST_HELP, "pipeline/list"),
        help=PIPELINE_LIST_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pipeline_list_parser.set_defaults(func=CmdPipelineList)
