import logging

import pytest

from dvc.main import main
from tests.func.metrics.utils import _write_json


@pytest.mark.parametrize(
    "command, metric_value",
    (("metrics", {"m": 1}), ("plots", [{"m": 1}, {"m": 2}])),
)
def test_diff_no_file_on_target_rev(
    tmp_dir, scm, dvc, caplog, command, metric_value
):
    with tmp_dir.branch("new_branch", new=True):
        _write_json(tmp_dir, metric_value, "metric.json")

        with caplog.at_level(logging.WARNING, "dvc"):
            assert (
                main([command, "diff", "master", "--targets", "metric.json"])
                == 0
            )

    assert "'metric.json' was not found at: 'master'." in caplog.text


@pytest.mark.parametrize(
    "command, malformed_metric",
    (("metrics", '{"m": 1'), ("plots", '[{"m": 1}, {"m": 2}'),),
)
def test_show_malformed_metric(
    tmp_dir, scm, dvc, caplog, command, malformed_metric
):
    tmp_dir.gen("metric.json", malformed_metric)

    with caplog.at_level(logging.ERROR, "dvc"):
        assert main([command, "show", "metric.json"]) == 1

    assert (
        f"Could not parse {command} files. "
        "Use `-v` option to see more details."
    ) in caplog.text


@pytest.mark.parametrize(
    "command, run_options",
    (("metrics", "-m/-M"), ("plots", "--plots/--plots-no-cache"),),
)
def test_show_no_metrics_files(tmp_dir, dvc, caplog, command, run_options):
    with caplog.at_level(logging.ERROR, "dvc"):
        assert main([command, "show"]) == 1

    assert (
        f"No {command} files in this repository. "
        f"Use `{run_options}` options for "
        f"`dvc run` to mark stage outputs as {command}."
    ) in caplog.text
