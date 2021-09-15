def test_diff_dirty(tmp_dir, scm, dvc, run_copy_metrics):
    (tmp_dir / "metric_t.json").dump([{"y": 2}, {"y": 3}], sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="init",
    )

    metric_head = [{"y": 3}, {"y": 5}]
    (tmp_dir / "metric_t.json").dump_json(metric_head, sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        commit="second",
    )

    metric_1 = [{"y": 5}, {"y": 6}]
    (tmp_dir / "metric_t.json").dump_json(metric_1, sort_keys=True)
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
    (tmp_dir / "metric.json").dump_json(metric_2, sort_keys=True)

    diff_result = dvc.plots.diff(props=props)
    assert diff_result == {
        "workspace": {
            "data": {"metric.json": {"data": metric_2, "props": props}}
        },
        "HEAD": {
            "data": {"metric.json": {"data": metric_head, "props": props}}
        },
    }
