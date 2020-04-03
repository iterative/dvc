import json

from bs4 import BeautifulSoup
from funcy import first


def _run_with_metric(tmp_dir, dvc, metric, metric_filename, commit=None):
    tmp_dir.gen({metric_filename: json.dumps(metric)})
    dvc.run(metrics_no_cache=[metric_filename])
    if hasattr(dvc, "scm"):
        dvc.scm.add([metric_filename, metric_filename + ".dvc"])
        if commit:
            dvc.scm.commit(commit)


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
