import pytest
from networkx import DiGraph
from networkx.utils import graphs_equal

from dvc.repo.graph import get_subgraph_of_nodes


@pytest.mark.parametrize(
    "nodes,downstream,expected_edges",
    [
        ([], False, {1: [2, 3], 2: [4, 5], 3: [6, 7], 8: [9]}),
        ([1], False, {1: [2, 3], 2: [4, 5], 3: [6, 7]}),
        ([2], False, {2: [4, 5]}),
        ([3], False, {3: [6, 7]}),
        ([8], False, [(8, 9)]),
        ([2, 3, 8], False, {2: [4, 5], 3: [6, 7], 8: [9]}),
        ([4], False, {4: []}),
        ([], True, {1: [2, 3], 2: [4, 5], 3: [6, 7], 8: [9]}),
        ([1], True, {1: []}),
        ([9], True, [(8, 9)]),
        ([2], True, [(1, 2)]),
        ([6], True, [(1, 3), (3, 6)]),
        ([2, 3, 8], True, {1: [2, 3], 8: []}),
        ([4, 7], True, {1: [2, 3], 2: [4], 3: [7]}),
    ],
)
def test_subgraph_of_nodes(nodes, downstream, expected_edges):
    r"""
             1
           /   \
          2     3      8
         / \   / \     |
        4   5 6   7    9
    """
    graph = DiGraph({1: [2, 3], 2: [4, 5], 3: [6, 7], 8: [9]})
    subgraph = get_subgraph_of_nodes(graph, nodes, downstream=downstream)
    expected = DiGraph(expected_edges)
    assert graphs_equal(expected, subgraph)
