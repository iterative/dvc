from itertools import chain

from pygtrie import Trie

from dvc.repo.index import Index
from dvc.stage import Stage
from dvc.utils import relpath
from tests.func.plots.utils import _write_json


def test_index(tmp_dir, scm, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.commit([s.outs[0].fspath for s in (stage1, stage2)], msg="add")

    index = Index(dvc)
    assert index.fs == dvc.fs

    assert len(index) == len(index.stages) == 2
    assert set(index.stages) == set(index) == {stage1, stage2}
    assert stage1 in index
    assert stage2 in index

    assert index.outs_graph
    assert index.graph
    assert index.build_graph()
    assert isinstance(index.outs_trie, Trie)

    assert index.identifier == "2ba7c7c5b395d4211348d6274b869fc7"
    index.check_graph()


def test_repr(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen("foo", "foo", commit="add foo")

    brancher = dvc.brancher([scm.get_rev()])
    rev = next(brancher)
    assert rev == "workspace"
    assert repr(Index(dvc)) == f"Index({dvc}, fs@{rev})"

    rev = next(brancher)
    assert rev == scm.get_rev()
    assert repr(Index(dvc)) == f"Index({dvc}, fs@{rev[:7]})"


def test_filter_index(tmp_dir, dvc, run_copy):
    tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", name="copy-foo-bar")

    def filter_pipeline(stage):
        return bool(stage.cmd)

    filtered_index = Index(dvc).filter(filter_pipeline)
    assert list(filtered_index) == [stage2]


def test_slice_index(tmp_dir, dvc):
    tmp_dir.gen({"dir1": {"foo": "foo"}, "dir2": {"bar": "bar"}})
    with (tmp_dir / "dir1").chdir():
        (stage1,) = dvc.add("foo")
    with (tmp_dir / "dir2").chdir():
        (stage2,) = dvc.add("bar")

    index = Index(dvc)

    sliced = index.slice("dir1")
    assert set(sliced) == {stage1}
    assert sliced.stages is not index.stages  # sanity check

    sliced = index.slice(tmp_dir / "dir1")
    assert set(sliced) == {stage1}

    sliced = index.slice("dir2")
    assert set(sliced) == {stage2}

    with (tmp_dir / "dir1").chdir():
        sliced = index.slice(relpath(tmp_dir / "dir2"))
        assert set(sliced) == {stage2}


def outputs_equal(actual, expected):
    actual, expected = list(actual), list(expected)

    def sort_fn(output):
        return output.fspath

    assert len(actual) == len(expected)
    pairs = zip(sorted(actual, key=sort_fn), sorted(expected, key=sort_fn))
    assert all(actual.fspath == expected.fspath for actual, expected in pairs)
    return True


def test_deps_outs_getters(tmp_dir, dvc, run_copy_metrics):
    (foo_stage,) = tmp_dir.dvc_gen({"foo": "foo"})
    tmp_dir.gen({"params.yaml": "param: 100\n"})
    tmp_dir.gen({"m_temp.yaml": str(5)})

    run_stage1 = run_copy_metrics(
        "m_temp.yaml",
        "m.yaml",
        metrics=["m.yaml"],
        params=["param"],
        name="copy-metrics",
    )
    _write_json(tmp_dir, [{"a": 1, "b": 2}, {"a": 2, "b": 3}], "metric_t.json")
    run_stage2 = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics2",
    )

    index = Index(dvc)

    stages = [foo_stage, run_stage1, run_stage2]
    (metrics,) = run_stage1.outs
    _, params = run_stage1.deps
    (plots,) = run_stage2.outs

    expected_outs = chain.from_iterable([stage.outs for stage in stages])
    expected_deps = chain.from_iterable([stage.deps for stage in stages])

    assert outputs_equal(index.outs, expected_outs)
    assert outputs_equal(index.deps, expected_deps)
    assert outputs_equal(index.decorated_outputs, [metrics, plots])
    assert outputs_equal(index.metrics, [metrics])
    assert outputs_equal(index.plots, [plots])
    assert outputs_equal(index.params, [params])


def test_add_update(dvc):
    """Test that add/update overwrites existing stages with the new ones.

    The old stages and the new ones might have same hash, so we are
    making sure that the old stages were removed and replaced by new ones
    using `id`/`is` checks.
    """
    index = Index(dvc)
    new_stage = Stage(dvc, path="path1")
    new_index = index.add(new_stage)

    assert not index.stages
    assert new_index.stages == [new_stage]

    dup_stage1 = Stage(dvc, path="path1")
    dup_stage2 = Stage(dvc, path="path2")
    dup_index = index.update([dup_stage1, dup_stage2])
    assert not index.stages
    assert len(new_index) == 1
    assert new_index.stages[0] is new_stage
    assert set(map(id, dup_index.stages)) == {id(dup_stage1), id(dup_stage2)}


def test_discard_remove(dvc):
    pass


def test_difference():
    pass


def test_used_objs():
    pass


def test_dumpd():
    pass


def test_unique_identifier():
    pass
