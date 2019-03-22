from __future__ import unicode_literals

from dvc.utils.compat import str

import os

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.command.base import CmdBase, fix_subparsers


class CmdPipelineShow(CmdBase):
    def _show(self, target, commands, outs):
        import networkx
        from dvc.stage import Stage

        stage = Stage.load(self.repo, target)
        G = self.repo.graph()[0]
        stages = networkx.get_node_attributes(G, "stage")
        node = os.path.relpath(stage.path, self.repo.root_dir)

        for n in networkx.dfs_postorder_nodes(G, node):
            if commands:
                logger.info(stages[n].cmd)
            elif outs:
                for out in stages[n].outs:
                    logger.info(str(out))
            else:
                logger.info(n)

    def __build_graph(self, target, commands, outs):
        import networkx
        from dvc.stage import Stage

        stage = Stage.load(self.repo, target)
        node = os.path.relpath(stage.path, self.repo.root_dir)

        pipelines = list(
            filter(lambda g: node in g.nodes(), self.repo.pipelines())
        )

        assert len(pipelines) == 1
        G = pipelines[0]
        stages = networkx.get_node_attributes(G, "stage")

        nodes = []
        for n in G.nodes():
            stage = stages[n]
            if commands:
                if stage.cmd is None:
                    continue
                nodes.append(stage.cmd)
            elif outs:
                for out in stage.outs:
                    nodes.append(str(out))
            else:
                nodes.append(stage.relpath)

        edges = []
        for e in G.edges():
            from_stage = stages[e[0]]
            to_stage = stages[e[1]]
            if commands:
                if to_stage.cmd is None:
                    continue
                edges.append((from_stage.cmd, to_stage.cmd))
            elif outs:
                for from_out in from_stage.outs:
                    for to_out in to_stage.outs:
                        edges.append((str(from_out), str(to_out)))
            else:
                edges.append((from_stage.relpath, to_stage.relpath))

        return nodes, edges

    def _show_ascii(self, target, commands, outs):
        from dvc.dagascii import draw

        nodes, edges = self.__build_graph(target, commands, outs)

        if not nodes:
            return

        draw(nodes, edges)

    def __write_dot(self, target, commands, outs, filename):
        import networkx
        from networkx.drawing.nx_pydot import write_dot

        _, edges = self.__build_graph(target, commands, outs)
        edges = [edge[::-1] for edge in edges]

        simple_g = networkx.DiGraph()
        simple_g.add_edges_from(edges)
        write_dot(simple_g, filename)

    def run(self, unlock=False):
        if not self.args.targets:
            self.args.targets = self.default_targets

        for target in self.args.targets:
            try:
                if self.args.ascii:
                    self._show_ascii(
                        target, self.args.commands, self.args.outs
                    )
                elif self.args.dot:
                    self.__write_dot(
                        target,
                        self.args.commands,
                        self.args.outs,
                        self.args.dot,
                    )
                else:
                    self._show(target, self.args.commands, self.args.outs)
            except DvcException:
                msg = "failed to show pipeline for '{}'".format(target)
                logger.error(msg)
                return 1
        return 0


class CmdPipelineList(CmdBase):
    def run(self):
        import networkx

        pipelines = self.repo.pipelines()
        for p in pipelines:
            stages = networkx.get_node_attributes(p, "stage")
            for stage in stages:
                logger.info(stage)
            if len(stages) != 0:
                logger.info("=" * 80)
        logger.info("{} pipeline(s) total".format(len(pipelines)))

        return 0


def add_parser(subparsers, parent_parser):
    PIPELINE_HELP = (
        "Manage pipeline.\ndocumentation: https://man.dvc.org/pipeline"
    )
    pipeline_parser = subparsers.add_parser(
        "pipeline",
        parents=[parent_parser],
        description=PIPELINE_HELP,
        help=PIPELINE_HELP,
    )

    pipeline_subparsers = pipeline_parser.add_subparsers(
        dest="cmd",
        help="Use dvc pipeline CMD --help for command-specific help.",
    )

    fix_subparsers(pipeline_subparsers)

    PIPELINE_SHOW_HELP = "Show pipeline."
    pipeline_show_parser = pipeline_subparsers.add_parser(
        "show",
        parents=[parent_parser],
        description=PIPELINE_SHOW_HELP,
        help=PIPELINE_SHOW_HELP,
    )
    pipeline_show_group = pipeline_show_parser.add_mutually_exclusive_group()
    pipeline_show_group.add_argument(
        "-c",
        "--commands",
        action="store_true",
        default=False,
        help="Print commands instead of paths to DVC files.",
    )
    pipeline_show_group.add_argument(
        "-o",
        "--outs",
        action="store_true",
        default=False,
        help="Print output files instead of paths to DVC files.",
    )
    pipeline_show_parser.add_argument(
        "--ascii",
        action="store_true",
        default=False,
        help="Output DAG as ASCII.",
    )
    pipeline_show_parser.add_argument(
        "--dot", help="Write DAG in .dot format."
    )
    pipeline_show_parser.add_argument(
        "targets", nargs="*", help="DVC files. 'Dvcfile' by default."
    )
    pipeline_show_parser.set_defaults(func=CmdPipelineShow)

    PIPELINE_LIST_HELP = "List pipelines."
    pipeline_list_parser = pipeline_subparsers.add_parser(
        "list",
        parents=[parent_parser],
        description=PIPELINE_LIST_HELP,
        help=PIPELINE_LIST_HELP,
    )
    pipeline_list_parser.set_defaults(func=CmdPipelineList)
