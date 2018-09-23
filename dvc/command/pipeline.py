import os

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase
from dvc.dagascii import Dagascii


class CmdPipelineShow(CmdBase):
    def _show(self, target, commands, outs):
        import networkx
        from dvc.stage import Stage

        stage = Stage.load(self.project, target)
        G = self.project.graph()[0]
        stages = networkx.get_node_attributes(G, 'stage')
        node = os.path.relpath(stage.path, self.project.root_dir)

        for n in networkx.dfs_postorder_nodes(G, node):
            if commands:
                self.project.logger.info(stages[n].cmd)
            elif outs:
                for out in stages[n].outs:
                    self.project.logger.info(str(out))
            else:
                self.project.logger.info(n)

    def __build_graph(self, target, commands, outs):
        import networkx
        from dvc.stage import Stage

        stage = Stage.load(self.project, target)
        node = os.path.relpath(stage.path, self.project.root_dir)

        pipelines = list(filter(lambda g: node in g.nodes(),
                                self.project.pipelines()))

        assert len(pipelines) == 1
        G = pipelines[0]
        stages = networkx.get_node_attributes(G, 'stage')

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
                        edges.append((str(from_out),
                                      str(to_out)))
            else:
                edges.append((from_stage.relpath, to_stage.relpath))

        return nodes, edges

    def _show_ascii(self, target, commands, outs):
        nodes, edges = self.__build_graph(target, commands, outs)

        if not nodes:
            return

        d = Dagascii(nodes, edges)
        d.draw()

    def __write_dot(self, target, commands, outs, filename):
        import networkx
        from networkx.drawing.nx_pydot import write_dot

        _, edges = self.__build_graph(target, commands, outs)
        edges = [edge[::-1] for edge in edges]

        simple_g = networkx.DiGraph()
        simple_g.add_edges_from(edges)
        write_dot(simple_g, filename)

    def run(self, unlock=False):
        for target in self.args.targets:
            try:
                if self.args.ascii:
                    self._show_ascii(target,
                                     self.args.commands,
                                     self.args.outs)
                elif self.args.dot:
                    self.__write_dot(target,
                                     self.args.commands,
                                     self.args.outs,
                                     self.args.dot)
                else:
                    self._show(target,
                               self.args.commands,
                               self.args.outs)
            except DvcException as ex:
                msg = 'Failed to show pipeline for \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                return 1
        return 0
