import pytest
from networkx import DiGraph
from networkx.utils import graphs_equal

from dvc.repo.graph import get_steps, get_subgraph_of_nodes


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


@pytest.mark.parametrize(
    "nodes,downstream,expected_steps",
    [
        ([], False, [4, 5, 2, 6, 7, 3, 1, 9, 8]),
        ([1], False, [4, 5, 2, 6, 7, 3, 1]),
        ([2], False, [4, 5, 2]),
        ([3], False, [6, 7, 3]),
        ([8], False, [9, 8]),
        ([2, 3, 8], False, [4, 5, 2, 6, 7, 3, 9, 8]),
        ([4], False, [4]),
        ([], True, [4, 5, 2, 6, 7, 3, 1, 9, 8]),
        ([1], True, [1]),
        ([9], True, [9, 8]),
        ([2], True, [2, 1]),
        ([6], True, [6, 3, 1]),
        ([2, 3, 8], True, [8, 2, 3, 1]),
        ([4, 7], True, [4, 2, 7, 3, 1]),
    ],
)
def test_steps(nodes, downstream, expected_steps):
    r"""
             1
           /   \
          2     3      8
         / \   / \     |
        4   5 6   7    9
    """
    graph = DiGraph({1: [2, 3], 2: [4, 5], 3: [6, 7], 8: [9]})
    assert get_steps(graph, nodes, downstream=downstream) == expected_steps
