import json
import logging
from copy import copy

import pytest
from bs4 import BeautifulSoup
from funcy import first

from dvc.exceptions import DvcException


def _run_with_metric(tmp_dir, metric, metric_filename, commit=None, tag=None):
    tmp_dir.gen({metric_filename: json.dumps(metric)})
    tmp_dir.dvc.run(metrics_no_cache=[metric_filename])
    if hasattr(tmp_dir.dvc, "scm"):
        tmp_dir.dvc.scm.add([metric_filename, metric_filename + ".dvc"])
        if commit:
            tmp_dir.dvc.scm.commit(commit)
        if tag:
            tmp_dir.dvc.scm.tag(tag)


def _add_revision(data, rev="current workspace"):
    new_data = copy(data)
    for e in new_data:
        e["revision"] = rev

    return new_data


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
    _run_with_metric(tmp_dir, metric, "metric.json", "first run")

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
    _run_with_metric(tmp_dir, confusion_matrix, "metric.json", "first run")
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
    _run_with_metric(tmp_dir, metric_1, "metric.json", "init", "v1")

    metric_2 = [{"x": 1, "y": 3}, {"x": 2, "y": 5}]
    _run_with_metric(tmp_dir, metric_2, "metric.json", "second", "v2")

    metric_3 = [{"x": 1, "y": 5}, {"x": 2, "y": 6}]
    _run_with_metric(tmp_dir, metric_3, "metric.json", "third")

    (tmp_dir / "template.dvct").write_text("<DVC_PLOT::metric.json>")
    dvc.plot("template.dvct", revisions=["HEAD", "v2", "v1"])

    content = BeautifulSoup((tmp_dir / "template.html").read_text())
    assert json.dumps(
        {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": [
                    {"x": 1, "y": 5, "revision": "HEAD"},
                    {"x": 2, "y": 6, "revision": "HEAD"},
                    {"x": 1, "y": 3, "revision": "v2"},
                    {"x": 2, "y": 5, "revision": "v2"},
                    {"x": 1, "y": 2, "revision": "v1"},
                    {"x": 2, "y": 3, "revision": "v1"},
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
    ) in first(content.body.script.contents)


def test_plot_even_if_metric_missing(tmp_dir, scm, dvc, caplog):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    metric = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    _run_with_metric(tmp_dir, metric, "metric.json", "there is metric", "v2")

    (tmp_dir / "template.dvct").write_text("<DVC_PLOT::metric.json>")

    caplog.clear()
    with caplog.at_level(logging.WARNING, "dvc"):
        result = dvc.plot("template.dvct", revisions=["v1", "v2"])
    assert (
        first(caplog.messages)
        == "File 'metric.json' was not found at: 'v1'. It will not be plotted."
    )

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    assert json.dumps(
        {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": [
                    {"x": 1, "y": 2, "revision": "v2"},
                    {"x": 2, "y": 3, "revision": "v2"},
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


def test_throw_on_no_metric_at_all(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    tmp_dir.scm_gen(
        "some_other_file",
        "other content",
        commit="there is no metric as well",
    )
    scm.tag("v2")

    (tmp_dir / "template.dvct").write_text("<DVC_PLOT::metric.json>")

    # TODO create exception
    with pytest.raises(DvcException):
        dvc.plot("template.dvct", revisions=["v2", "v1"])
