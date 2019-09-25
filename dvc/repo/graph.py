from __future__ import unicode_literals


def check_acyclic(graph):
    import networkx as nx
    from dvc.exceptions import CyclicGraphError

    cycles = list(nx.simple_cycles(graph))

    if cycles:
        raise CyclicGraphError(cycles[0])


def get_pipeline(pipelines, node):
    found = [i for i in pipelines if i.has_node(node)]
    assert len(found) == 1
    return found[0]


def get_pipelines(G):
    import networkx as nx

    return [G.subgraph(c).copy() for c in nx.weakly_connected_components(G)]


def get_stages(G):
    import networkx

    return list(networkx.get_node_attributes(G, "stage").values())
