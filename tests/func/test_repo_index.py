import os
from itertools import chain

import pytest
from pygtrie import Trie

from dvc.repo.index import Index
from dvc.stage import PipelineStage, Stage
from dvc.utils import relpath


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
    assert isinstance(index.outs_trie, Trie)
    assert index.identifier
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
    (tmp_dir / "metric_t.json").dump_json(
        [{"a": 1, "b": 2}, {"a": 2, "b": 3}], sort_keys=True
    )
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
    assert outputs_equal(index.decorated_outs, [metrics, plots])
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


def assert_index_equal(first, second, strict=True, ordered=True):
    assert len(first) == len(second), "Index have different no. of stages"
    assert set(first) == set(second), "Index does not have same stages"
    if ordered:
        assert list(first) == list(
            second
        ), "Index does not have same sequence of stages"
    if strict:
        assert set(map(id, first)) == set(
            map(id, second)
        ), "Index is not strictly equal"


def test_discard_remove(dvc):
    stage = Stage(dvc, path="path1")
    index = Index(dvc, stages=[stage])

    assert list(index.discard(Stage(dvc, "path2"))) == list(index)
    new_index = index.discard(stage)
    assert len(new_index) == 0

    with pytest.raises(ValueError):
        index.remove(Stage(dvc, "path2"))
    assert index.stages == [stage]
    assert list(index.remove(stage)) == []


def test_difference(dvc):
    stages = [Stage(dvc, path=f"path{i}") for i in range(10)]
    index = Index(dvc, stages=stages)

    new_index = index.difference([*stages[:5], Stage(dvc, path="path100")])
    assert index.stages == stages
    assert set(new_index) == set(stages[5:])


def test_dumpd(dvc):
    stages = [
        PipelineStage(dvc, "dvc.yaml", name="stage1"),
        Stage(dvc, "path"),
    ]
    index = Index(dvc, stages=stages)
    assert index.dumpd() == {"dvc.yaml:stage1": {}, "path": {}}
    assert index.identifier == "d43da84e9001540c26abf2bf4541c275"


def test_unique_identifier(tmp_dir, dvc, scm, run_copy):
    dvc.scm_context.autostage = True
    tmp_dir.dvc_gen("foo", "foo")
    run_copy("foo", "bar", name="copy-foo-bar")

    revs = []
    n_commits = 5
    for i in range(n_commits):
        # create a few empty commits
        scm.commit(f"commit {i}")
        revs.append(scm.get_rev())
    assert len(set(revs)) == n_commits  # the commit revs should be unique

    ids = []
    for _ in dvc.brancher(revs=revs):
        index = Index(dvc)
        assert index.stages
        ids.append(index.identifier)

    # we get "workspace" as well from the brancher by default
    assert len(revs) + 1 == len(ids)
    possible_ids = {
        True: "2ba7c7c5b395d4211348d6274b869fc7",
        False: "8406970ad2fcafaa84d9310330a67576",
    }
    assert set(ids) == {possible_ids[os.name == "posix"]}


def test_skip_graph_checks(dvc, mocker):
    # See https://github.com/iterative/dvc/issues/2671 for more info
    mock_build_graph = mocker.spy(Index.graph, "fget")

    # sanity check
    Index(dvc).check_graph()
    assert mock_build_graph.called
    mock_build_graph.reset_mock()

    # check that our hack can be enabled
    dvc._skip_graph_checks = True
    Index(dvc).check_graph()
    assert not mock_build_graph.called
    mock_build_graph.reset_mock()

    # check that our hack can be disabled
    dvc._skip_graph_checks = False
    Index(dvc).check_graph()
    assert mock_build_graph.called


def get_index(dvc, rev):
    if rev:
        brancher = dvc.brancher(revs=[rev])
        if rev != "workspace":
            assert next(brancher) == "workspace"
        next(brancher)
    return Index(dvc)


@pytest.mark.parametrize("rev", ["workspace", "HEAD"])
def test_used_objs(tmp_dir, scm, dvc, run_copy, rev):
    from dvc.hash_info import HashInfo

    dvc.config["core"]["autostage"] = True
    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}}, "foo": "foo"})
    run_copy("foo", "bar", name="copy-foo-bar")
    scm.commit("commit")

    index = get_index(dvc, rev)

    expected_objs = [
        HashInfo(
            name="md5",
            value="acbd18db4cc2f85cedef654fccc4a4d8",
            obj_name="bar",
        ),
        HashInfo(
            name="md5",
            value="8c7dd922ad47494fc02c388e12c00eac",
            obj_name="dir/subdir/file",
        ),
        HashInfo(
            name="md5",
            value="d28c9e28591aeb7e303dc6772ffa6f6b.dir",
            obj_name="dir",
        ),
    ]

    assert index.used_objs() == {None: set(expected_objs)}
    assert index.used_objs("dir") == {None: set(expected_objs[1:])}
    assert index.used_objs(".", recursive=True) == {None: set(expected_objs)}
    assert index.used_objs("copy-foo-bar", with_deps=True) == {
        None: {expected_objs[0]}
    }
