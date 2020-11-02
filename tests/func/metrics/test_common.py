import logging

import pytest

from tests.func.metrics.utils import _write_json


def metrics_diff(dvc, filename, revision):
    dvc.metrics.diff(targets=[filename], a_rev=revision)


def plots_diff(dvc, filename, revision):
    dvc.plots.diff(targets=[filename], revs=[revision])


@pytest.mark.parametrize(
    "diff_fun, metric_value",
    ((metrics_diff, {"m": 1}), (plots_diff, [{"m": 1}, {"m": 2}])),
)
def test_diff_no_file_on_target_rev(
    tmp_dir, scm, dvc, caplog, diff_fun, metric_value
):
    with tmp_dir.branch("new_branch", new=True):
        _write_json(tmp_dir, metric_value, "metric.json")

        with caplog.at_level(logging.WARNING, "dvc"):
            diff_fun(dvc, "metric.json", "master")

    assert "'metric.json' was not found at: 'master'." in caplog.text


@pytest.mark.parametrize(
    "fun, malformed_metric",
    (
        (lambda repo: repo.metrics, '{"m": 1'),
        (lambda repo: repo.plots, '[{"m": 1}, {"m": 2}'),
    ),
)
def test_show_malformed_metric(tmp_dir, scm, dvc, fun, malformed_metric):
    tmp_dir.gen("metric.json", malformed_metric)
    _write_json(
        tmp_dir, {"val": 1, "val2": [{"p": 1}, {"p": 2}]}, "metric_proper.json"
    )

    fun(dvc).show(targets=["metric.json", "metric_proper.json"])
