import logging

import pytest

from dvc.exceptions import NoMetricsFoundError, NoMetricsParsedError
from tests.func.metrics.utils import _write_json


@pytest.mark.parametrize(
    "diff, metric_value",
    (
        (
            lambda repo, target, rev: repo.metrics.diff(
                targets=[target], a_rev=rev
            ),
            {"m": 1},
        ),
        (
            lambda repo, target, rev: repo.plots.diff(
                targets=[target], revs=[rev]
            ),
            [{"m": 1}, {"m": 2}],
        ),
    ),
)
def test_diff_no_file_on_target_rev(
    tmp_dir, scm, dvc, caplog, diff, metric_value
):
    with tmp_dir.branch("new_branch", new=True):
        _write_json(tmp_dir, metric_value, "metric.json")

        with caplog.at_level(logging.WARNING, "dvc"):
            diff(dvc, "metric.json", "master")

    assert "'metric.json' was not found at: 'master'." in caplog.text


@pytest.mark.parametrize(
    "show, malformed_metric",
    (
        (lambda repo, target: repo.metrics.show(targets=[target]), '{"m": 1'),
        (
            lambda repo, target: repo.plots.show(targets=[target]),
            '[{"m": 1}, {"m": 2}',
        ),
    ),
)
def test_show_malformed_metric(
    tmp_dir, scm, dvc, caplog, show, malformed_metric
):
    tmp_dir.gen("metric.json", malformed_metric)

    with pytest.raises(NoMetricsParsedError):
        show(dvc, "metric.json")


@pytest.mark.parametrize(
    "show",
    (lambda repo: repo.metrics.show(), lambda repo: repo.plots.show(),),
)
def test_show_no_metrics_files(tmp_dir, dvc, caplog, show):
    with pytest.raises(NoMetricsFoundError):
        show(dvc)
