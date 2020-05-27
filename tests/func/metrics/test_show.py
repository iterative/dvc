import pytest

from dvc.repo.metrics.show import NoMetricsError


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
        "working tree": {"metrics.yaml": {"foo": 1}},
        "branch": {"metrics.yaml": {"foo": 2}},
    }
