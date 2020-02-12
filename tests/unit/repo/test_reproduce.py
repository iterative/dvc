from dvc.repo.reproduce import _get_active_graph


def test_get_active_graph(tmp_dir, dvc):
    (pre_foo_stage,) = tmp_dir.dvc_gen({"pre-foo": "pre-foo"})
    foo_stage = dvc.run(deps=["pre-foo"], outs=["foo"], cmd="echo foo > foo")
    bar_stage = dvc.run(deps=["foo"], outs=["bar"], cmd="echo bar > bar")
    baz_stage = dvc.run(deps=["foo"], outs=["baz"], cmd="echo baz > baz")

    dvc.lock_stage("bar.dvc")

    graph = dvc.graph
    active_graph = _get_active_graph(graph)
    assert active_graph.nodes == graph.nodes
    assert set(active_graph.edges) == {
        (foo_stage, pre_foo_stage),
        (baz_stage, foo_stage),
    }

    dvc.lock_stage("baz.dvc")

    graph = dvc.graph
    active_graph = _get_active_graph(graph)
    assert set(active_graph.nodes) == {bar_stage, baz_stage}
    assert not active_graph.edges
