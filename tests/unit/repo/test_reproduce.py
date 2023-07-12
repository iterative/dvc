from itertools import chain

from networkx import DiGraph
from networkx.utils import graphs_equal

from dvc.repo.reproduce import get_active_graph, plan_repro


def test_active_graph(mocker):
    n = mocker.sentinel
    n1, n2, n3, n4, n5, n6, n7, n8, n9 = (getattr(n, f"n{i}") for i in range(1, 10))
    edges = {n1: [n2, n3], n2: [n4, n5], n3: [n6, n7], n8: [n9]}
    for node in chain.from_iterable([n, *v] for n, v in edges.items()):
        node.frozen = False

    g = DiGraph(edges)

    active = get_active_graph(g)
    assert graphs_equal(g, active)

    n2.frozen = True
    active = get_active_graph(g)
    assert g.edges() - active.edges() == {(n2, n5), (n2, n4)}
    assert n2 in active
    assert not active.edges() - g.edges()
    assert not graphs_equal(g, active)


def test_repro_plan(M):
    r"""
             1
           /  \
          2    3    8
         / \  / \   |
        4  5 6  7   9
    """
    g = DiGraph({1: [2, 3], 2: [4, 5], 3: [6, 7], 8: [9]})

    assert plan_repro(g) == [4, 5, 2, 6, 7, 3, 1, 9, 8]
    assert plan_repro(g, [1]) == [4, 5, 2, 6, 7, 3, 1]
    assert plan_repro(g, [4], downstream=True) == [4, 2, 1]
    assert plan_repro(g, [8], True) == plan_repro(g, [9], True) == [9, 8]
    assert plan_repro(g, [2, 8], True) == [4, 5, 2, 6, 7, 3, 1, 9, 8]
    assert plan_repro(g, [2, 3], downstream=True) == [
        M.any_of(2, 3),
        M.any_of(2, 3),
        1,
    ]


def test_number_reproduces(tmp_dir, dvc, mocker):
    mock = mocker.Mock(return_value=None)
    tmp_dir.dvc_gen({"pre-foo": "pre-foo"})

    dvc.stage.add(name="echo-foo", outs=["foo"], cmd="echo foo > foo", verify=False)
    dvc.stage.add(
        name="echo-bar", deps=["foo"], outs=["bar"], cmd="echo bar > bar", verify=False
    )
    dvc.stage.add(
        name="echo-baz", deps=["foo"], outs=["baz"], cmd="echo baz > baz", verify=False
    )
    dvc.stage.add(
        name="echo-boop",
        deps=["bar"],
        outs=["boop"],
        cmd="echo boop > boop",
        verify=False,
    )

    dvc.reproduce(all_pipelines=True, repro_fn=mock)
    assert mock.call_count == 5
