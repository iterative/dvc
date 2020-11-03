import os

import pytest

from dvc.repo import Repo
from dvc.utils.fs import remove


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


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_metric(tmp_dir, scm, use_dvc):
    tmp_dir.gen("metrics.yaml", "foo: 1.1")

    if use_dvc:
        dvc = Repo.init()
    else:
        dvc = Repo(uninitialized=True)

    assert dvc.metrics.show(targets=["metrics.yaml"]) == {
        "": {"metrics.yaml": {"foo": 1.1}}
    }

    if not use_dvc:
        assert not (tmp_dir / ".dvc").exists()


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_metric_branch(tmp_dir, scm, use_dvc):
    tmp_dir.scm_gen("metrics.yaml", "foo: 1.1", commit="init")
    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("metrics.yaml", "foo: 2.2", commit="other")

    if use_dvc:
        dvc = Repo.init()
    else:
        dvc = Repo(uninitialized=True)

    assert dvc.metrics.show(targets=["metrics.yaml"], revs=["branch"]) == {
        "workspace": {"metrics.yaml": {"foo": 1.1}},
        "branch": {"metrics.yaml": {"foo": 2.2}},
    }

    if not use_dvc:
        assert not (tmp_dir / ".dvc").exists()


def test_non_metric_and_recurisve_show(tmp_dir, dvc, run_copy_metrics):
    tmp_dir.gen(
        {"metrics_t.yaml": "foo: 1.1", "metrics": {"metric1.yaml": "bar: 1.2"}}
    )

    metric2 = os.fspath(tmp_dir / "metrics" / "metric2.yaml")
    run_copy_metrics("metrics_t.yaml", metric2, metrics=[metric2])

    assert dvc.metrics.show(
        targets=["metrics_t.yaml", "metrics"], recursive=True
    ) == {
        "": {
            os.path.join("metrics", "metric1.yaml"): {"bar": 1.2},
            os.path.join("metrics", "metric2.yaml"): {"foo": 1.1},
            "metrics_t.yaml": {"foo": 1.1},
        }
    }


def test_show_falsey(tmp_dir, dvc):
    tmp_dir.gen("metrics.json", '{"foo": 0, "bar": 0.0, "baz": {}}')
    assert dvc.metrics.show(targets=["metrics.json"]) == {
        "": {"metrics.json": {"foo": 0, "bar": 0.0}}
    }


def test_show_no_repo(tmp_dir):
    tmp_dir.gen("metrics.json", '{"foo": 0, "bar": 0.0, "baz": {}}')

    dvc = Repo(uninitialized=True)

    dvc.metrics.show(targets=["metrics.json"])
