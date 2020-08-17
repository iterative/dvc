def check_acyclic(graph):
    import networkx as nx

    from dvc.exceptions import CyclicGraphError

    try:
        edges = nx.find_cycle(graph, orientation="original")
    except nx.NetworkXNoCycle:
        return

    stages = set()
    for from_node, to_node, _ in edges:
        stages.add(from_node)
        stages.add(to_node)

    raise CyclicGraphError(list(stages))


def get_pipeline(pipelines, node):
    found = [i for i in pipelines if i.has_node(node)]
    assert len(found) == 1
    return found[0]


def get_pipelines(G):
    import networkx as nx

    return [G.subgraph(c).copy() for c in nx.weakly_connected_components(G)]
