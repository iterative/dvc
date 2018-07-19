import os

from dvc.exceptions import DvcException
from dvc.command.base import CmdBase


class CmdPipelineShow(CmdBase):
    def _show(self, target, commands, outs):
        import networkx
        from dvc.project import NotDvcFileError

        G = self.project.graph()[0]
        stages = networkx.get_node_attributes(G, 'stage')
        node = os.path.relpath(os.path.abspath(target), self.project.root_dir)
        if node not in stages:
            raise NotDvcFileError(node)

        for n in networkx.dfs_postorder_nodes(G, node):
            if commands:
                self.project.logger.info(stages[n].cmd)
            elif outs:
                for out in stages[n].outs:
                    self.project.logger.info(out.rel_path)
            else:
                self.project.logger.info(n)

    def run(self, unlock=False):
        for target in self.args.targets:
            try:
                self._show(target, self.args.commands, self.args.outs)
            except DvcException as ex:
                msg = 'Failed to show pipeline for \'{}\''.format(target)
                self.project.logger.error(msg, ex)
                return 1
        return 0
