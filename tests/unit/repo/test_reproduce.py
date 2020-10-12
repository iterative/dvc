import mock

from dvc.repo.reproduce import _get_active_graph


def test_get_active_graph(tmp_dir, dvc):
    (pre_foo_stage,) = tmp_dir.dvc_gen({"pre-foo": "pre-foo"})
    foo_stage = dvc.run(
        single_stage=True, deps=["pre-foo"], outs=["foo"], cmd="echo foo > foo"
    )
    bar_stage = dvc.run(
        single_stage=True, deps=["foo"], outs=["bar"], cmd="echo bar > bar"
    )
    baz_stage = dvc.run(
        single_stage=True, deps=["foo"], outs=["baz"], cmd="echo baz > baz"
    )

    dvc.freeze("bar.dvc")

    graph = dvc.graph
    active_graph = _get_active_graph(graph)
    assert active_graph.nodes == graph.nodes
    assert set(active_graph.edges) == {
        (foo_stage, pre_foo_stage),
        (baz_stage, foo_stage),
    }

    dvc.freeze("baz.dvc")

    graph = dvc.graph
    active_graph = _get_active_graph(graph)
    assert set(active_graph.nodes) == {bar_stage, baz_stage}
    assert not active_graph.edges


@mock.patch("dvc.repo.reproduce._reproduce_stage", returns=[])
def test_number_reproduces(reproduce_stage_mock, tmp_dir, dvc):
    tmp_dir.dvc_gen({"pre-foo": "pre-foo"})

    dvc.run(
        single_stage=True, deps=["pre-foo"], outs=["foo"], cmd="echo foo > foo"
    )
    dvc.run(
        single_stage=True, deps=["foo"], outs=["bar"], cmd="echo bar > bar"
    )
    dvc.run(
        single_stage=True, deps=["foo"], outs=["baz"], cmd="echo baz > baz"
    )
    dvc.run(
        single_stage=True, deps=["bar"], outs=["boop"], cmd="echo boop > boop"
    )

    reproduce_stage_mock.reset_mock()

    dvc.reproduce(all_pipelines=True)

    assert reproduce_stage_mock.call_count == 5
