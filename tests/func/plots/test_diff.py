import json

import pytest

from dvc.repo.plots.data import INDEX_FIELD, REVISION_FIELD
from tests.func.plots.utils import _write_json


@pytest.mark.skip
def test_diff_dirty(tmp_dir, scm, dvc, run_copy_metrics):

    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
    )

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="second",
    )

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric_t.json")
    run_copy_metrics(
        "metric_t.json", "metric.json", plots_no_cache=["metric.json"]
    )

    plot_string = dvc.plots.diff(props={"fields": {"y"}})["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 3, INDEX_FIELD: 0, REVISION_FIELD: "HEAD"},
        {"y": 5, INDEX_FIELD: 1, REVISION_FIELD: "HEAD"},
        {"y": 5, INDEX_FIELD: 0, REVISION_FIELD: "workspace"},
        {"y": 6, INDEX_FIELD: 1, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"

    _write_json(tmp_dir, [{"y": 7}, {"y": 8}], "metric.json")

    plot_string = dvc.plots.diff(props={"fields": {"y"}})["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 3, INDEX_FIELD: 0, REVISION_FIELD: "HEAD"},
        {"y": 5, INDEX_FIELD: 1, REVISION_FIELD: "HEAD"},
        {"y": 7, INDEX_FIELD: 0, REVISION_FIELD: "workspace"},
        {"y": 8, INDEX_FIELD: 1, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"
