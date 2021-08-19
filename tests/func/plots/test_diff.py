from tests.func.plots.utils import _write_json


def test_diff_dirty(tmp_dir, scm, dvc, run_copy_metrics):
    _write_json(tmp_dir, [{"y": 2}, {"y": 3}], "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
    )

    metric_head = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_head, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="second",
    )

    metric_1 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_1, "metric_t.json")
    run_copy_metrics(
        "metric_t.json", "metric.json", plots_no_cache=["metric.json"]
    )
    props = {"fields": {"y"}}
    diff_result = dvc.plots.diff(props=props)
    assert diff_result == {
        "workspace": {
            "data": {"metric.json": {"data": metric_1, "props": props}}
        },
        "HEAD": {
            "data": {"metric.json": {"data": metric_head, "props": props}}
        },
    }
    metric_2 = [{"y": 7}, {"y": 8}]
    _write_json(tmp_dir, metric_2, "metric.json")

    diff_result = dvc.plots.diff(props=props)
    assert diff_result == {
        "workspace": {
            "data": {"metric.json": {"data": metric_2, "props": props}}
        },
        "HEAD": {
            "data": {"metric.json": {"data": metric_head, "props": props}}
        },
    }
