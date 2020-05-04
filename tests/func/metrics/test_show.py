import pytest

from dvc.repo.metrics.show import NoMetricsError


def test_show_empty(dvc):
    with pytest.raises(NoMetricsError):
        dvc.metrics.show()


def test_show_simple(tmp_dir, dvc):
    tmp_dir.gen("metrics.yaml", "1.1")
    dvc.run(metrics=["metrics.yaml"], single_stage=True)
    assert dvc.metrics.show() == {"": {"metrics.yaml": 1.1}}


def test_show(tmp_dir, dvc):
    tmp_dir.gen("metrics.yaml", "foo: 1.1")
    dvc.run(metrics=["metrics.yaml"], single_stage=True)
    assert dvc.metrics.show() == {"": {"metrics.yaml": {"foo": 1.1}}}


def test_show_multiple(tmp_dir, dvc):
    tmp_dir.gen("foo", "foo: 1\n")
    tmp_dir.gen("baz", "baz: 2\n")
    dvc.run(fname="foo.dvc", metrics=["foo"], single_stage=True)
    dvc.run(fname="baz.dvc", metrics=["baz"], single_stage=True)
    assert dvc.metrics.show() == {"": {"foo": {"foo": 1}, "baz": {"baz": 2}}}


def test_show_invalid_metric(tmp_dir, dvc):
    tmp_dir.gen("metrics.yaml", "foo:\n- bar\n- baz\nxyz: string")
    dvc.run(metrics=["metrics.yaml"], single_stage=True)
    with pytest.raises(NoMetricsError):
        dvc.metrics.show()


def test_show_branch(tmp_dir, scm, dvc):
    tmp_dir.gen("metrics.yaml", "foo: 1")
    dvc.run(metrics_no_cache=["metrics.yaml"], single_stage=True)
    scm.add(["metrics.yaml", "metrics.yaml.dvc"])
    scm.commit("init")

    with tmp_dir.branch("branch", new=True):
        tmp_dir.scm_gen("metrics.yaml", "foo: 2", commit="branch")

    assert dvc.metrics.show(revs=["branch"]) == {
        "working tree": {"metrics.yaml": {"foo": 1}},
        "branch": {"metrics.yaml": {"foo": 2}},
    }
