from itertools import chain

import pytest
from pygtrie import Trie

from dvc.repo.index import Index
from dvc.stage import Stage


def test_index(tmp_dir, scm, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.commit([s.outs[0].fspath for s in (stage1, stage2)], msg="add")

    index = Index.from_repo(dvc)

    assert set(index.stages) == {stage1, stage2}
    assert index.outs_graph
    assert index.graph
    assert isinstance(index.outs_trie, Trie)
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

    index = Index.from_repo(dvc)

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


def test_update(dvc):
    """Test that update overwrites existing stages with the new ones.

    The old stages and the new ones might have same hash, so we are
    making sure that the old stages were removed and replaced by new ones
    using `id`/`is` checks.
    """
    index = Index.from_repo(dvc)
    new_stage = Stage(dvc, path="path1")
    new_index = index.update({new_stage})

    assert not index.stages
    assert new_index.stages == [new_stage]

    dup_stage1 = Stage(dvc, path="path1")
    dup_stage2 = Stage(dvc, path="path2")
    dup_index = index.update([dup_stage1, dup_stage2])
    assert not index.stages
    assert len(new_index.stages) == 1
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


def test_used_objs(tmp_dir, scm, dvc, run_copy):
    from dvc_data.hashfile.hash_info import HashInfo

    dvc.scm_context.autostage = True
    tmp_dir.dvc_gen({"dir": {"subdir": {"file": "file"}}, "foo": "foo"})
    run_copy("foo", "bar", name="copy-foo-bar")
    scm.commit("commit")

    for _ in dvc.brancher(revs=["HEAD"]):
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

        assert dvc.index.used_objs() == {None: set(expected_objs)}
        assert dvc.index.used_objs("dir") == {None: set(expected_objs[1:])}
        assert dvc.index.used_objs(".", recursive=True) == {None: set(expected_objs)}
        assert dvc.index.used_objs("copy-foo-bar", with_deps=True) == {
            None: {expected_objs[0]}
        }


def test_view_granular_dir(tmp_dir, scm, dvc, run_copy):
    tmp_dir.dvc_gen(
        {"dir": {"subdir": {"in-subdir": "in-subdir"}, "in-dir": "in-dir"}},
        commit="init",
    )
    index = Index.from_repo(dvc)

    # view should include the specific target, parent dirs, and children
    # view should exclude any siblings of the target
    view = index.targets_view("dir/subdir")

    assert view.data_keys == {
        "repo": {
            ("dir", "subdir"),
        }
    }

    data_index = view.data["repo"]
    assert ("dir",) in data_index
    assert (
        "dir",
        "subdir",
    ) in data_index
    assert ("dir", "subdir", "in-subdir") in data_index
    assert (
        "dir",
        "in-dir",
    ) not in data_index


def test_view_onerror(tmp_dir, scm, dvc):
    from dvc.exceptions import NoOutputOrStageError

    tmp_dir.dvc_gen({"foo": "foo"}, commit="init")
    index = Index.from_repo(dvc)

    with pytest.raises(NoOutputOrStageError):
        index.targets_view(["foo", "missing"])

    failed = []

    def onerror(target, exc):
        failed.append((target, exc))

    view = index.targets_view(["foo", "missing"], onerror=onerror)
    data = view.data["repo"]

    assert len(failed) == 1
    target, exc = failed[0]
    assert target == "missing"
    assert isinstance(exc, NoOutputOrStageError)
    assert len(data) == 1
    assert data[("foo",)]


def test_view_stage_filter(tmp_dir, scm, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.commit([s.outs[0].fspath for s in (stage1, stage2)], msg="add")
    index = Index.from_repo(dvc)

    view = index.targets_view(None)
    assert set(view.stages) == {stage1, stage2}
    assert {out.fs_path for out in view.outs} == {
        out.fs_path for out in (stage1.outs + stage2.outs)
    }

    view = index.targets_view(
        None, stage_filter=lambda s: getattr(s, "name", "").startswith("copy")
    )
    assert set(view.stages) == {stage2}
    assert {out.fs_path for out in view.outs} == {out.fs_path for out in stage2.outs}


def test_view_outs_filter(tmp_dir, scm, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.commit([s.outs[0].fspath for s in (stage1, stage2)], msg="add")
    index = Index.from_repo(dvc)

    view = index.targets_view(None, outs_filter=lambda o: o.def_path == "foo")
    assert set(view.stages) == {stage1, stage2}
    assert {out.fs_path for out in view.outs} == {out.fs_path for out in stage1.outs}


def test_view_combined_filter(tmp_dir, scm, dvc, run_copy):
    (stage1,) = tmp_dir.dvc_gen("foo", "foo")
    stage2 = run_copy("foo", "bar", name="copy-foo-bar")
    tmp_dir.commit([s.outs[0].fspath for s in (stage1, stage2)], msg="add")
    index = Index.from_repo(dvc)

    view = index.targets_view(
        None,
        stage_filter=lambda s: getattr(s, "name", "").startswith("copy"),
        outs_filter=lambda o: o.def_path == "foo",
    )
    assert set(view.stages) == {stage2}
    assert set(view.outs) == set()

    view = index.targets_view(
        None,
        stage_filter=lambda s: getattr(s, "name", "").startswith("copy"),
        outs_filter=lambda o: o.def_path == "bar",
    )
    assert set(view.stages) == {stage2}
    assert {out.fs_path for out in view.outs} == {out.fs_path for out in stage2.outs}


def test_view_brancher(tmp_dir, scm, dvc):
    tmp_dir.dvc_gen({"foo": "foo"}, commit="init")
    index = Index.from_repo(dvc)

    for _ in dvc.brancher(revs=["HEAD"]):
        view = index.targets_view("foo")
        data = view.data["repo"]
        assert data[("foo",)]


def test_with_gitignore(tmp_dir, dvc, scm):
    (stage,) = tmp_dir.dvc_gen({"data": {"foo": "foo", "bar": "bar"}})

    index = Index.from_repo(dvc)
    assert index.stages == [stage]

    scm.ignore(stage.path)
    scm._reset()

    index = Index.from_repo(dvc)
    assert not index.stages


def test_ignored_dir_unignored_pattern(tmp_dir, dvc, scm):
    tmp_dir.gen({".gitignore": "data/**\n!data/**/\n!data/**/*.dvc"})
    scm.add([".gitignore"])
    (stage,) = tmp_dir.dvc_gen({"data/raw/tracked.csv": "5,6,7,8"})
    index = Index.from_repo(dvc)
    assert index.stages == [stage]
