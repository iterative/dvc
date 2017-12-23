import os
import networkx as nx

from dvc.command.common.base import CmdBase


class CmdShowPipeline(CmdBase):
    def draw(self, g, target, fname_suffix):
        fname = 'pipeline_' + fname_suffix
        try:
            A = nx.nx_agraph.to_agraph(g)
            A.write(fname + '.dot')
            A.draw(fname + '.jpeg', format='jpeg', prog='dot')
        except Exception as exc:
            Logger.error('Failed to draw dependency graph for {}: {}'.format(target, exc))
            return 1

        return 0

    def find_sub(self, fname):
        for s in self.subs:
            if fname in s.nodes():
                return s

        return self.g

    def draw_targets(self, target):
        if not target:
            target = '.'
            return self.draw(self.g, target, target)

        for t in self.args.target:
            fname_suffix = os.path.basename(os.path.normpath(t))
            s = self.find_sub(t)

            ret = self.draw(s, t, fname_suffix)
            if ret != 0:
                return ret

        return 0

    def run(self):
        self.g = nx.DiGraph()
        self.subs = []

        saved_targets = self.args.target
        self.args.target = ['.']
 
        for stage in self.project.stages():
            self.collect_stage(stage)

        self.args.target = saved_targets

        # Try to find independent clusters which might occure
        # when a bunch of data items were used independently.
        self.subs = nx.weakly_connected_component_subgraphs(self.g)

        return self.draw_targets(saved_targets)

    def collect_stage(self, stage):
        name = os.path.relpath(stage.path, self.project.root_dir)
        state = StateFile.load(data_item, self.git)

        self.g.add_node(name)

        for dep in stage.deps:
            i = os.path.relpath(dep.path, self.project.root_dir)
            self.g.add_node(i)
            self.g.add_edge(i, name)

        for out in state.outs:
            o = os.path.relpath(out.path, self.project.root_dir)
            self.g.add_node(o)
            self.g.add_edge(name, o)
