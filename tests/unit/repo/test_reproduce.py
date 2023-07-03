from itertools import chain

from networkx import DiGraph

from dvc.repo.reproduce import plan_repro


def test_number_reproduces(tmp_dir, dvc, mocker):
    reproduce_stage_mock = mocker.patch(
        "dvc.repo.reproduce._reproduce_stage", returns=[]
    )
    tmp_dir.dvc_gen({"pre-foo": "pre-foo"})

    dvc.run(name="echo-foo", outs=["foo"], cmd="echo foo > foo")
    dvc.run(name="echo-bar", deps=["foo"], outs=["bar"], cmd="echo bar > bar")
    dvc.run(name="echo-baz", deps=["foo"], outs=["baz"], cmd="echo baz > baz")
    dvc.run(name="echo-boop", deps=["bar"], outs=["boop"], cmd="echo boop > boop")

    reproduce_stage_mock.reset_mock()

    dvc.reproduce(all_pipelines=True)

    assert reproduce_stage_mock.call_count == 5


def test_repro_plan(mocker):
    r"""
             n1
           /   \
          n2    m3    n8
         / \   / \     |
        n4 n5 n6 n7    n9
    """

    # note: downstream steps may not be stable, use AnyOf in such cases
    class AnyOf:
        def __init__(self, *items):
            self.items = items

        def __eq__(self, other: object) -> bool:
            return any(item == other for item in self.items)

    n = mocker.sentinel
    n1, n2, n3, n4, n5, n6, n7, n8, n9 = (getattr(n, f"n{i}") for i in range(1, 10))
    edges = {n1: [n2, n3], n2: [n4, n5], n3: [n6, n7], n8: [n9]}
    for node in chain.from_iterable([n, *v] for n, v in edges.items()):
        node.frozen = False

    g = DiGraph(edges)
    assert plan_repro(g) == [n4, n5, n2, n6, n7, n3, n1, n9, n8]
    assert plan_repro(g, [n1]) == [n4, n5, n2, n6, n7, n3, n1]
    assert plan_repro(g, [n4], downstream=True) == [n4, n2, n1]
    assert plan_repro(g, [n8], True) == plan_repro(g, [n9], True) == [n9, n8]
    assert plan_repro(g, [n2, n8], True) == [n4, n5, n2, n6, n7, n3, n1, n9, n8]
    assert plan_repro(g, [n2, n3], downstream=True) == [
        AnyOf(n2, n3),
        AnyOf(n2, n3),
        n1,
    ]

    n2.frozen = True
    assert plan_repro(g) == [n2, n6, n7, n3, n1, n9, n8, n4, n5]
    assert plan_repro(g, [n1]) == [n2, n6, n7, n3, n1]
    assert plan_repro(g, [n4], downstream=True) == [n4]
    assert plan_repro(g, [n2, n8], pipeline=True) == [n2, n6, n7, n3, n1, n9, n8]
    assert plan_repro(g, [n2, n3], downstream=True) == [
        AnyOf(n2, n3),
        AnyOf(n2, n3),
        n1,
    ]
