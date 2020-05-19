import json

from .test_plots import PlotData, _run_with, _write_json


def test_diff_dirty(tmp_dir, scm, dvc):
    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric.json")
    _run_with(tmp_dir, plots_no_cache=["metric.json"], commit="init")

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric.json")
    _run_with(tmp_dir, plots_no_cache=["metric.json"], commit="second")

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric.json")
    _run_with(tmp_dir, plots_no_cache=["metric.json"])

    plot_string = dvc.plots.diff(fields={"y"})["metric.json"]

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 5, PlotData.INDEX_FIELD: 0, "rev": "working tree"},
        {"y": 6, PlotData.INDEX_FIELD: 1, "rev": "working tree"},
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"
