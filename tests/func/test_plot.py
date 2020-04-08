import csv
import json
import logging
import shutil

import pytest
from bs4 import BeautifulSoup
from funcy import first

from dvc.compat import fspath
from dvc.exceptions import DvcException


def _run_with_metric(tmp_dir, metric_filename, commit=None, tag=None):
    # tmp_dir.gen({metric_filename: json.dumps(metric)})
    tmp_dir.dvc.run(metrics_no_cache=[metric_filename])
    if hasattr(tmp_dir.dvc, "scm"):
        tmp_dir.dvc.scm.add([metric_filename, metric_filename + ".dvc"])
        if commit:
            tmp_dir.dvc.scm.commit(commit)
        if tag:
            tmp_dir.dvc.scm.tag(tag)


def _write_csv(metric, filename):
    with open(filename, "w") as csvobj:
        if all([len(e) > 1 for e in metric]):
            writer = csv.DictWriter(
                csvobj, fieldnames=list(first(metric).keys())
            )
            writer.writeheader()
            writer.writerows(metric)
        else:
            writer = csv.writer(csvobj)
            for d in metric:
                assert len(d) == 1
                writer.writerow(list(d.values()))


def _write_json(tmp_dir, metric, filename):
    tmp_dir.gen(filename, json.dumps(metric))


def test_plot_csv_one_column(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    _write_csv(metric, "metric.csv")
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    result = dvc.plot("metric.csv")
    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            # TODO csv reads as strings, what to do with that?
            {"y": "2", "x": 0, "rev": "current"},
            {"y": "3", "x": 1, "rev": "current"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


def test_plot_csv_multiple_columns(tmp_dir, scm, dvc):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_csv(metric, "metric.csv")
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    result = dvc.plot("metric.csv")
    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            # header was skipped so index starts at 1
            {"y": "2", "x": 1, "rev": "current"},
            {"y": "3", "x": 2, "rev": "current"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


def test_plot_json_single_val(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    result = dvc.plot("metric.json")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"y": 2, "x": 0, "rev": "current"},
            {"y": 3, "x": 1, "rev": "current"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


def test_plot_json_multiple_val(tmp_dir, scm, dvc):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 100, "val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    result = dvc.plot("metric.json")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"y": 2, "x": 0, "rev": "current"},
            {"y": 3, "x": 1, "rev": "current"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


@pytest.mark.skip
def test_plot_confusion(tmp_dir, dvc):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    _run_with_metric(tmp_dir, confusion_matrix, "metric.json", "first run")
    template_content = "<DVC_PLOT::metric.json::cf.json>"
    (tmp_dir / "template.dvct").write_text(template_content)

    result = dvc.plot_template("template.dvct")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    assert json.dumps(
        {
            "$schema": "https://vega.github.io/schema/vega-lite/v4.json",
            "data": {
                "values": [
                    {"predicted": "B", "actual": "A", "rev": "current"},
                    {"predicted": "A", "actual": "A", "rev": "current"},
                ]
            },
            "mark": "rect",
            "encoding": {
                "x": {
                    "field": "predicted",
                    "type": "nominal",
                    "sort": "ascending",
                },
                "y": {
                    "field": "actual",
                    "type": "nominal",
                    "sort": "ascending",
                },
                "color": {"aggregate": "count", "type": "quantitative"},
                "facet": {"field": "rev", "type": "nominal"},
            },
            "title": "metric.json",
        },
        indent=4,
        separators=(",", ": "),
    ) in first(page_content.body.script.contents)


def test_plot_multiple_revs(tmp_dir, scm, dvc):
    metric_1 = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    _write_json(tmp_dir, metric_1, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    metric_2 = [{"x": 1, "y": 3}, {"x": 2, "y": 5}]
    _write_json(tmp_dir, metric_2, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "second", "v2")

    metric_3 = [{"x": 1, "y": 5}, {"x": 2, "y": 6}]
    _write_json(tmp_dir, metric_3, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "third")

    dvc.plot("metric.json", revisions=["HEAD", "v2", "v1"])

    content = BeautifulSoup((tmp_dir / "default.html").read_text())
    vega_data = json.dumps(
        [
            {"y": 5, "x": 0, "rev": "HEAD"},
            {"y": 6, "x": 1, "rev": "HEAD"},
            {"y": 3, "x": 0, "rev": "v2"},
            {"y": 5, "x": 1, "rev": "v2"},
            {"y": 2, "x": 0, "rev": "v1"},
            {"y": 3, "x": 1, "rev": "v1"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(content.body.script.contents)


def test_plot_even_if_metric_missing(tmp_dir, scm, dvc, caplog):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    metric = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "there is metric", "v2")

    caplog.clear()
    with caplog.at_level(logging.WARNING, "dvc"):
        result = dvc.plot("metric.json", revisions=["v1", "v2"])
    assert (
        first(caplog.messages)
        == "File 'metric.json' was not found at: 'v1'. It will not be plotted."
    )

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [{"y": 2, "x": 0, "rev": "v2"}, {"y": 3, "x": 1, "rev": "v2"}],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


def test_throw_on_no_metric_at_all(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    tmp_dir.scm_gen(
        "some_other_file",
        "other content",
        commit="there is no metric as well",
    )
    scm.tag("v2")

    # TODO create exception
    with pytest.raises(DvcException):
        dvc.plot("metric.json", revisions=["v2", "v1"])


def test_custom_template(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.dvct"),
        fspath(tmp_dir / "newtemplate.dvct"),
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    result = dvc.plot("metric.json", "newtemplate.dvct")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"a": 1, "b": 2, "rev": "current"},
            {"a": 2, "b": 3, "rev": "current"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


def _replace(path, src, dst):
    path.write_text(path.read_text().replace(src, dst))


def test_custom_template_with_specified_data(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.dvct"),
        fspath(tmp_dir / "newtemplate.dvct"),
    )
    _replace(
        tmp_dir / "newtemplate.dvct",
        "DVC_METRIC_DATA",
        "DVC_METRIC_DATA::metric.json",
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    result = dvc.plot(datafile=None, template="newtemplate.dvct")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"a": 1, "b": 2, "rev": "current"},
            {"a": 2, "b": 3, "rev": "current"},
        ],
        indent=4,
        separators=(",", ": "),
    )
    assert vega_data in first(page_content.body.script.contents)


# TODO
# test for pure json template
# def test_plot_in_html_file(tmp_dir):
# def test_plot_override_data_file
# def test_plot_custom_template_file / test_
# def test_plot_multiple_plots
