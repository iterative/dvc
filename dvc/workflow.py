import networkx as nx

from dvc.exceptions import DvcException


class WorkflowError(DvcException):
    def __init__(self, msg):
        super(WorkflowError, self).__init__('Workflow error: ' + msg)


class Commit(object):
    TEXT_LIMIT = 30
    DVC_REPRO_PREFIX = 'DVC repro'
    COLLAPSED_TEXT = DVC_REPRO_PREFIX + '\n<< collapsed commits >>'

    def __init__(self, hash, parents, name, date, comment, is_target):
        self._hash = hash
        self._parent_hashes = set(parents.split())
        self._name = name
        self._date = date
        self._comment = comment
        self._is_target = is_target
        self._is_collapsed = False

    @property
    def hash(self):
        return self._hash

    @property
    def parent_hashes(self):
        return self._parent_hashes

    def add_parents(self, parent_hashes):
        self._parent_hashes |= parent_hashes

    def remove_parent(self, hash):
        self._parent_hashes.remove(hash)

    @property
    def text(self):
        if self._is_collapsed:
            return self.COLLAPSED_TEXT
        return self._comment[:self.TEXT_LIMIT] + '\n' + self.hash

    @property
    def is_repro(self):
        return self._comment.startswith(self.DVC_REPRO_PREFIX)

    def make_colapsed(self):
        self._is_collapsed = True


class Workflow(object):
    def __init__(self, target, merges_map, root='', no_repro_commits=True):
        self._target = target
        self._merges_map = merges_map

        self._commits = {}
        self._root = root

        self._edges = {}
        self._back_edges = {}

        self._no_repro_commits = no_repro_commits
        pass

    def add_commit(self, commit):
        self._commits[commit.hash] = commit

        for p in commit.parent_hashes:
            if p not in self._edges:
                self._edges[p] = set()
            self._edges[p].add(commit.hash)

        if commit.hash not in self._back_edges:
            self._back_edges[commit.hash] = set()
        for p in commit.parent_hashes:
            self._back_edges[commit.hash].add(p)
        pass

    def build_graph(self):
        g = nx.DiGraph(name='DVC Workflow', directed=False)

        if self._no_repro_commits:
            self.collapse_repro_commits()

        for hash in set(self._edges.keys() + self._back_edges.keys()):
            commit = self._commits[hash]
            g.add_node(hash,
                       attr_dict={
                           'label': commit.text,
                           'color': 'red' if commit.hash == '85d03b2' else 'black',
                           'weight': 8 if commit.hash == '411ea7c' else 18
                       }
            )

        for commit in self._commits.values():
            for p in commit.parent_hashes:
                g.add_edge(commit.hash, p)

        A = nx.nx_agraph.to_agraph(g)
        fname = 'workflow.jpeg'
        A = A.to_undirected()
        A.write(fname + '.dot')
        A.draw(fname, format='jpeg', prog='dot')
        pass

    def _build_graph(self, curr, g):
        if '' not in self._commits or curr.hash not in self._commits:
            return
        next = self._commits[curr.hash]

        g.add_node(next.hash)
        g.add_edge(curr.hash, next.hash)

        self._build_graph(next, g)

    def collapse_repro_commits(self):
        hashes_to_remove = []
        for commit in self._commits.values():
            if commit.is_repro:
                if self._remove_or_collapse(commit):
                    hashes_to_remove.append(commit.hash)

        for hash in hashes_to_remove:
            del self._commits[hash]
            del self._edges[hash]
            del self._back_edges[hash]
        pass

    def _remove_or_collapse(self, commit):
        parent_commit_hashes = self._back_edges[commit.hash]
        child_commit_hashes = self._edges.get(commit.hash)

        if child_commit_hashes and all(self._commits[hash].is_repro for hash in child_commit_hashes):
            for hash in child_commit_hashes:
                self._commits[hash].add_parents(commit.parent_hashes)
                self._commits[hash].remove_parent(commit.hash)

            self._redirect_edges(self._edges, child_commit_hashes, commit.hash, parent_commit_hashes)
            self._redirect_edges(self._back_edges, parent_commit_hashes, commit.hash, child_commit_hashes)

            return True
        else:
            commit.make_colapsed()
            return False
        pass

    @staticmethod
    def _redirect_edges(edges, child_commit_hashes, hash, commit_hashes):
        for h in commit_hashes:
            edges[h].remove(hash)
            edges[h] |= child_commit_hashes
