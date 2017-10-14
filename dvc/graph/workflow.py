import networkx as nx
import json
from dateutil.parser import parse

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.graph.workflow_templates import TOP, BOTTOM


class WorkflowError(DvcException):
    def __init__(self, msg):
        super(WorkflowError, self).__init__('Workflow error: ' + msg)


class CommitCollapseStrategy(object):
    def __init__(self, workflow):
        self._workflow = workflow
        pass

    def is_change_needed(self, commit, child_commits):
        raise NotImplementedError()

    def is_remove_needed(self, commit):
        raise NotImplementedError()

    def make_collapsed(self, commit):
        commit.make_collapsed()

    def upstream(self, commit):
        for child in self._workflow.child_commits(commit.hash):
            self.upstream_to_child(commit, child)
        pass

    def upstream_to_child(self, commit, child):
        if commit.has_target_metric:
            if not child.has_target_metric:
                child.set_target_metric(commit.target_metric)
        if commit.branch_tips:
            child.add_branch_tips(commit.branch_tips)


class CollapseDvcReproCommitsStrategy(CommitCollapseStrategy):
    def __init__(self, workflow):
        super(CollapseDvcReproCommitsStrategy, self).__init__(workflow)

    def is_change_needed(self, commit, hash):
        return commit.is_repro

    def is_remove_needed(self, commit):
        child_commits = self._workflow.child_commits(commit.hash)
        return child_commits and all(ch.is_repro for ch in child_commits)


class CollapseNotMeticsCommitsStrategy(CommitCollapseStrategy):
    def __init__(self, workflow):
        super(CollapseNotMeticsCommitsStrategy, self).__init__(workflow)

    def is_change_needed(self, commit, hash):
        return not commit.has_target_metric

    def is_remove_needed(self, commit):
        child_commits = self._workflow.child_commits(commit.hash)
        return len(child_commits) == 1 and not commit.has_target_metric

    def upstream_to_child(self, commit, child):
        super(CollapseNotMeticsCommitsStrategy, self).upstream_to_child(commit, child)
        if not commit.is_repro:
            child.add_collapsed_commit(commit)

    def make_collapsed(self, commit):
        pass


class Workflow(object):
    def __init__(self, target, merges_map, branches_map=None):
        self._target = target
        self._merges_map = merges_map
        self._branches_map = branches_map

        self._commits = {}
        self._root_hash = None

        self._edges = {}
        self._back_edges = {}
        pass

    def add_commit(self, commit):
        self._commits[commit.hash] = commit

        for p in commit.parent_hashes:
            if p not in self._edges:
                self._edges[p] = set()
            self._edges[p].add(commit.hash)
            if commit.hash not in self._back_edges:
                self._back_edges[commit.hash] = set()
            self._back_edges[commit.hash].add(p)

        if not commit.parent_hashes:
            self._root_hash = commit.hash
        pass

    def get_commit(self, hash):
        return self._commits.get(hash)

    def child_commits(self, hash):
        if hash not in self._edges:
            return []
        return [self._commits[h] for h in self._edges[hash]]

    def build_graph(self, show_dvc_commits, show_all_commits, max_commits):
        self.modify_workflow(show_all_commits, show_dvc_commits)

        # g = nx.DiGraph(name='DVC Workflow', directed=False)

        nodes = []
        for hash in set(self._edges.keys() + self._back_edges.keys()):
            commit = self._commits[hash]

            d = parse(commit._date)
            print(d)
            node = {"id": int(hash, 16),
                    "color": self.node_color(commit),
                    "sequence": int(d.strftime("%s")),  # ????
                    # "verticalLevel": len(nodes),
                    # "horizontalLevel": 0,
                    "strings": commit.text(max_commits).split('\n')
                    }

            if commit._is_target and commit._target_metric:
                node["targetNumber"] = '{}'.format(commit.target_metric_delta)

            nodes.append(node)

            # g.add_node(hash,
            #            attr_dict={
            #                'label': commit.text(max_commits),
            #                'color': self.node_color(commit)
            #            }
            # )

        links = []
        for commit in self._commits.values():
            for p in commit.parent_hashes:
                links.append({"target": int(commit.hash, 16), "source": int(p, 16)})
                # g.add_edge(commit.hash, p)

        fname = 'workflow.html'
        with open(fname, 'w') as fd:
            graph = {"nodes": nodes, "links": links}
            fd.write(TOP + "\n  var data = " + json.dumps(graph, indent=4) + "\n" + BOTTOM)
            fd.close()

        # A = nx.nx_agraph.to_agraph(g)
        # fname = 'workflow'
        # A = A.to_undirected()
        # A.write(fname + '.dot')
        # A.draw(fname + '.jpeg', format='jpeg', prog='dot')
        pass

    def modify_workflow(self, show_all_commits, show_dvc_commits):
        self.derive_target_metric_deltas()
        if not show_all_commits:
            self.collapse_commits(CollapseDvcReproCommitsStrategy(self))
            if not show_dvc_commits:
                self.collapse_commits(CollapseNotMeticsCommitsStrategy(self))

    @staticmethod
    def node_color(commit):
        if commit.target_metric_delta is not None:
            if commit.target_metric_delta >= 0:
                return 'green'
            if commit.target_metric_delta < 0:
                return 'red'
        return 'black'

    def _build_graph(self, curr, g):
        if '' not in self._commits or curr.hash not in self._commits:
            return
        next = self._commits[curr.hash]

        g.add_node(next.hash)
        g.add_edge(curr.hash, next.hash)

        self._build_graph(next, g)

    def collapse_commits(self, strategy):
        hashes_to_remove = []
        for commit in self._commits.values():
            if strategy.is_change_needed(commit, commit.hash):
                if self._remove_or_collapse(commit, strategy):
                    hashes_to_remove.append(commit.hash)

        self.remove_hashes(hashes_to_remove)

    def remove_hashes(self, hashes_to_remove):
        for hash in hashes_to_remove:
            del self._commits[hash]
            if hash in self._edges:
                del self._edges[hash]
            if hash in self._back_edges:
                del self._back_edges[hash]
        pass

    def _remove_or_collapse(self, commit, strategy):
        parent_commit_hashes = self._back_edges.get(commit.hash)
        child_commit_hashes = self._edges.get(commit.hash)

        if child_commit_hashes is None:
            return False

        if strategy.is_remove_needed(commit):
            strategy.upstream(commit)

            for hash in child_commit_hashes:
                self._commits[hash].add_parents(commit.parent_hashes)
                self._commits[hash].remove_parent(commit.hash)

            self._redirect_edges(self._edges, child_commit_hashes, commit.hash, parent_commit_hashes)
            self._redirect_edges(self._back_edges, parent_commit_hashes, commit.hash, child_commit_hashes)

            return True
        else:
            strategy.make_collapsed(commit)
            return False
        pass

    @staticmethod
    def _redirect_edges(edges, child_commit_hashes, hash, commit_hashes):
        if commit_hashes:
            for h in commit_hashes:
                edges[h].remove(hash)
                if child_commit_hashes:
                    edges[h] |= child_commit_hashes

    def derive_target_metric_deltas(self):
        if not self._target:
            return

        if self._root_hash not in self._commits:
            Logger.warn('Cannot derive target metrics deltas: root commit was not found')
            return

        current_target_metric = None
        commit = self._commits[self._root_hash]
        self._traverse_target_metric(commit, current_target_metric)
        pass

    def _traverse_target_metric(self, commit, current_target_metric):
        new_target_metric = current_target_metric
        if commit.target_metric is not None:
            new_target_metric = commit.target_metric
            if current_target_metric is None:
                commit.set_target_metric_delta(0.0)
            else:
                commit.set_target_metric_delta(new_target_metric - current_target_metric)

        if commit.hash in self._edges:
            for hash in self._edges[commit.hash]:
                self._traverse_target_metric(self._commits[hash], new_target_metric)
        pass
