import os

import pytest

from dvc.repo import Repo
from dvc.repo.metrics.show import NoMetricsError
from dvc.utils.fs import remove


def test_show_empty(dvc):
    with pytest.raises(NoMetricsError):
        dvc.metrics.show()


def test_show_simple(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_t.yaml", "1.1")
    run_copy_metrics(
        "metrics_t.yaml", "metrics.yaml", metrics=["metrics.yaml"],
    )
    assert dvc.metrics.show() == {"": {"metrics.yaml": 1.1}}


def test_show(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_t.yaml", "foo: 1.1")
    run_copy_metrics(
        "metrics_t.yaml", "metrics.yaml", metrics=["metrics.yaml"],
    )
    assert dvc.metrics.show() == {"": {"metrics.yaml": {"foo": 1.1}}}


def test_show_multiple(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("foo_temp", "foo: 1\n")
    tmp_dir.gen("baz_temp", "baz: 2\n")
    run_copy_metrics("foo_temp", "foo", fname="foo.dvc", metrics=["foo"])
    run_copy_metrics("baz_temp", "baz", fname="baz.dvc", metrics=["baz"])
    assert dvc.metrics.show() == {"": {"foo": {"foo": 1}, "baz": {"baz": 2}}}


def test_show_invalid_metric(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_temp.yaml", "foo:\n- bar\n- baz\nxyz: string")
    run_copy_metrics(
        "metrics_temp.yaml", "metrics.yaml", metrics=["metrics.yaml"]
    )
    with pytest.raises(NoMetricsError):
        dvc.metrics.show()


def test_show_branch(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_temp.yaml", "foo: 1")
    run_copy_metrics(
        "metrics_temp.yaml", "metrics.yaml", metrics_no_cache=["metrics.yaml"]
    )
    scm.add(["metrics.yaml", "metrics.yaml.dvc"])
    scm.commit("init")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("metrics.yaml", "foo: 2", commit="branch")

    assert dvc.metrics.show(revs=["branch"]) == {
        "workspace": {"metrics.yaml": {"foo": 1}},
        "branch": {"metrics.yaml": {"foo": 2}},
    }


def test_show_subrepo_with_preexisting_tags(tmp_dir, scm):
    tmp_dir.gen("foo", "foo")
    scm.add("foo")
    scm.commit("init")
    scm.tag("no-metrics")

    tmp_dir.gen({"subdir": {}})
    subrepo_dir = tmp_dir / "subdir"
    with subrepo_dir.chdir():
        dvc = Repo.init(subdir=True)
        scm.commit("init dvc")

        dvc.run(
            cmd="echo foo: 1 > metrics.yaml",
            metrics=["metrics.yaml"],
            single_stage=True,
        )

    scm.add(
        [
            str(subrepo_dir / "metrics.yaml"),
            str(subrepo_dir / "metrics.yaml.dvc"),
        ]
    )
    scm.commit("init metrics")
    scm.tag("v1")

    expected_path = os.path.join("subdir", "metrics.yaml")
    assert dvc.metrics.show(all_tags=True) == {
        "workspace": {expected_path: {"foo": 1}},
        "v1": {expected_path: {"foo": 1}},
    }


def test_missing_cache(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen("metrics_t.yaml", "1.1")
    run_copy_metrics(
        "metrics_t.yaml", "metrics.yaml", metrics=["metrics.yaml"],
    )

    # This one should be skipped
    stage = run_copy_metrics(
        "metrics_t.yaml", "metrics2.yaml", metrics=["metrics2.yaml"],
    )
    remove(stage.outs[0].fspath)
    remove(stage.outs[0].cache_path)

    assert dvc.metrics.show() == {"": {"metrics.yaml": 1.1}}
