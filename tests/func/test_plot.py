import json
from copy import copy

from bs4 import BeautifulSoup
from funcy import first


def _run_with_metric(tmp_dir, dvc, metric, metric_filename, commit=None):
    tmp_dir.gen({metric_filename: json.dumps(metric)})
    dvc.run(metrics_no_cache=[metric_filename])
    if hasattr(dvc, "scm"):
        dvc.scm.add([metric_filename, metric_filename + ".dvc"])
        if commit:
            dvc.scm.commit(commit)


def _add_revision(data, rev="current workspace"):
    new_data = copy(data)
    for e in new_data:
        e["revision"] = rev


def to_data(rev_data):
    result = []
    for key, data in rev_data.items():
        result.extend(_add_revision(data, key))
    return result

# TODO
def test_plot_in_html_file(tmp_dir):
    pass


def test_plot_in_no_html(tmp_dir, scm, dvc):
    metric = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    _run_with_metric(tmp_dir, dvc, metric, "metric.json", "first run")

    template_content = "<DVC_PLOT::metric.json>"
    (tmp_dir / "template.dvct").write_text(template_content)

    result = dvc.plot("template.dvct")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    assert json.dumps(
        {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": [
                    {"x": 1, "y": 2, "revision": "current workspace"},
                    {"x": 2, "y": 3, "revision": "current workspace"},
                ]
            },
            "mark": {"type": "line"},
            "encoding": {
                "x": {"field": "x", "type": "quantitative"},
                "y": {"field": "y", "type": "quantitative"},
                "color": {"field": "revision", "type": "nominal"},
            },
            "title": "metric.json",
        },
        indent=4,
        separators=(",", ": "),
    ) in first(page_content.body.script.contents)


def test_plot_confusion(tmp_dir, dvc):
    confusion_matrix = [{"x": "B", "y": "A"}, {"x": "A", "y": "A"}]
    _run_with_metric(
        tmp_dir, dvc, confusion_matrix, "metric.json", "first run"
    )
    template_content = "<DVC_PLOT::metric.json::cf.json>"
    (tmp_dir / "template.dvct").write_text(template_content)

    result = dvc.plot("template.dvct")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    assert json.dumps(
        {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": [
                    {"x": "B", "y": "A", "revision": "current workspace"},
                    {"x": "A", "y": "A", "revision": "current workspace"},
                ]
            },
            "mark": "rect",
            "encoding": {
                "x": {
                    "field": "x",
                    "type": "nominal",
                    "sort": "ascending",
                    "title": "Predicted value",
                },
                "y": {
                    "field": "y",
                    "type": "nominal",
                    "sort": "ascending",
                    "title": "Actual value",
                },
                "color": {"aggregate": "count", "type": "quantitative"},
            },
            "title": "metric.json",
        },
        indent=4,
        separators=(",", ": "),
    ) in first(page_content.body.script.contents)


def test_plot_multiple_revisions(tmp_dir, scm, dvc):
    metric_1 = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    tmp_dir.scm_gen({"metric.json": json.dumps(metric_1)}, commit="init")
    scm.tag("v1")

    metric_2 = [{"x": 1, "y": 3}, {"x": 2, "y": 5}]
    tmp_dir.scm_gen(
        {"metric.json": json.dumps(metric_2)}, commit="2nd ver metric"
    )
    scm.tag("v2")

    metric_3 = [{"x": 1, "y": 5}, {"x": 2, "y": 6}]
    tmp_dir.scm_gen({"metric.json": json.dumps(metric_3)}, commit="new metric")

    dvc.plot(
        ["metric.json"],
        revisions=["HEAD", "v2", "v1"],
        plot_path="result.html",
    )
    page = tmp_dir / "result.html"

    assert page.exists()

    all_data = to_data({"HEAD": metric_3, "v2": metric_2, "v1": metric_1})
    expected_script_content = json.dumps(
        Template(dvc.dvc_dir).fill(all_data, "metric.json"),
        indent=4,
        separators=(",", ": "),
    )

    page_content = BeautifulSoup(page.read_text())
    assert expected_script_content in first(page_content.body.script.contents)
