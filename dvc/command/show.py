import os
import networkx as nx

from dvc.command.traverse import Traverse
from dvc.command.init import CmdInit

from dvc.logger import Logger
from dvc.state_file import StateFile

class CmdShow(Traverse):
    def __init__(self, settings):
        super(CmdShow, self).__init__(settings, "collect", do_not_start_from_root=False)
        self.g = nx.DiGraph()
        self.subs = []

    def draw(self, g, target, fname):
        try:
            A = nx.nx_agraph.to_agraph(g)
            A.write(fname + '.dot')
            A.draw(fname + '.svg', prog='dot')
        except Exception as exc:
            Logger.error('Failed to draw dependency graph for {}: {}'.format(target, exc))
            return 1

        return 0

    def find_sub(self, fname):
        for s in self.subs:
            if fname in s.nodes():
                return s

        return self.g

    def draw_targets(self):
        if self.parsed_args.target == None:
            target = self.settings.config.data_dir
            return self.draw(self.g, target, target)

        for t in self.parsed_args.target:
            fname = os.path.basename(os.path.normpath(t))
            s = self.find_sub(t)

            ret = self.draw(s, t, fname)
            if ret != 0:
                return ret

        return 0

    def run(self):
        saved_targets = self.settings.parsed_args.target
        self.settings.parsed_args.target = [self.settings.config.data_dir]
 
        ret = super(CmdShow, self).run()

        self.settings.parsed_args.target = saved_targets

        if ret != 0:
            Logger.error('Failed to build dependency graph for the project')
            return 1

        # Try to find independent clusters which might occure
        # when a bunch of data items were used independently.
        self.subs = nx.weakly_connected_component_subgraphs(self.g)

        return self.draw_targets()

    def process_file(self, target):
        data_item = self._get_data_item(target)
        name = data_item.data.relative
        state = StateFile.load(data_item.state.relative, self.git)

        self.g.add_node(name)

        for i in state.input_files:
            self.g.add_node(i)
            self.g.add_edge(i, name)

        for o in state.output_files:
            if o == name:
                continue
            self.g.add_node(o)
            self.g.add_edge(name, o)

    @property
    def no_git_actions(self):
        return True

    @staticmethod
    def not_committed_changes_warning():
        pass
