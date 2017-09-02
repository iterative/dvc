import networkx as nx

from dvc.exceptions import DvcException


class WorkflowError(DvcException):
    def __init__(self, msg):
        super(WorkflowError, self).__init__('Workflow error: ' + msg)


class Commit(object):
    def __init__(self, hash, parent_hash, name, date, comment, is_target):
        self._hash = hash
        self._parent_hash = parent_hash
        self._name = name
        self._date = date
        self._comment = comment
        self._is_target = is_target

    @property
    def hash(self):
        return self._hash


class Workflow(object):
    '''
    Options:
    --all   -   all commits
    //--no-repro - by default (no DVC commits)
    --no-dvc    - no DVC commits
    
    '''

    COMMIT_TEXT_LIMIT = 20

    def __init__(self, target, merges_map):
        self._target = target
        self._merges_map = merges_map

        self._commits = {}
        self._root = {}
        pass

    def add_commit(self, hash, parent_hash, name, date, comment, is_target):
        # Extract params to the Commit class?
        print 'add_commit %s' % (hash)
        self._commits[hash] = Commit(hash, parent_hash, name, date, comment, is_target)
        if not parent_hash:
            self._root = hash
        pass

    def build_graph(self):
        g = nx.DiGraph(name='DVC Workflow', directed=False)

        if not self._root:
            raise WorkflowError('cannot find root commit')

        # self._build_graph(self._commits[self._root], g)
        visited = set()
        for commit in self._commits.values():
            if commit.hash not in visited:
                g.add_node(commit.hash,
                           attr_dict={
                               'label': commit._comment[:Workflow.COMMIT_TEXT_LIMIT] + '\n' + commit.hash,
                                'color': 'red' if commit.hash == '85d03b2' else 'black',
                                'weight': 8 if commit.hash == '411ea7c' else 18
                           }
                )
                visited.add(commit.hash)
            if commit._parent_hash:
                g.add_edge(commit.hash, commit._parent_hash)
                if commit.hash == '411ea7c':
                    g.add_edge(commit.hash, commit._parent_hash, attr_dict={
                        'color': 'green'
                    })
                    g.add_edge(commit.hash, commit._parent_hash, attr_dict={
                        'color': 'red'
                    })

        A = nx.nx_agraph.to_agraph(g)
        fname = 'workflow'
        A = A.to_undirected()
        A.write(fname + '.dot')
        A.draw(fname, format='jpeg', prog='dot')
        pass

    def _build_graph(self, curr, g):
        if '' not in self._commits or curr.hash not in self._commits:
            return
        next = self._commits[curr.hash]

        print 'add node %s' % next.hash
        g.add_node(next.hash)
        print 'add edge (%s, %s)' % (curr.hash, next.hash)
        g.add_edge(curr.hash, next.hash)

        self._build_graph(next, g)
