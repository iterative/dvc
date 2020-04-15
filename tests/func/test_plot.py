import csv
import json
import logging
import shutil
from collections import OrderedDict

import pytest
from bs4 import BeautifulSoup
from funcy import first

from dvc.compat import fspath
from dvc.plot import (
    DefaultLinearTemplate,
    TemplateNotFound,
    NoDataForTemplateError,
)
from dvc.repo.plot import (
    NoMetricInHistoryError,
    NoDataOrTemplateProvided,
    PlotMetricTypeError,
)


def _remove_whitespace(value):
    return value.replace(" ", "").replace("\n", "")


def _run_with_metric(tmp_dir, metric_filename, commit=None, tag=None):
    tmp_dir.dvc.run(metrics_no_cache=[metric_filename])
    if hasattr(tmp_dir.dvc, "scm"):
        tmp_dir.dvc.scm.add([metric_filename, metric_filename + ".dvc"])
        if commit:
            tmp_dir.dvc.scm.commit(commit)
        if tag:
            tmp_dir.dvc.scm.tag(tag)


def _write_csv(metric, filename):
    with open(filename, "w", newline="") as csvobj:
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
    tmp_dir.gen(filename, json.dumps(metric, sort_keys=True))


def test_plot_csv_one_column(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    _write_csv(metric, "metric.csv")
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    result = dvc.plot("metric.csv")
    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"y": "2", "x": 0, "rev": "workspace"},
            {"y": "3", "x": 1, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_csv_multiple_columns(tmp_dir, scm, dvc):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    _write_csv(metric, "metric.csv")
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    result = dvc.plot("metric.csv")
    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            # header was skipped so index starts at 1
            {"y": "2", "x": 1, "rev": "workspace"},
            {"y": "3", "x": 2, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_json_single_val(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    result = dvc.plot("metric.json")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"y": 2, "x": 0, "rev": "workspace"},
            {"y": 3, "x": 1, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_json_multiple_val(tmp_dir, scm, dvc):
    metric = [
        {"first_val": 100, "val": 2},
        {"first_val": 200, "val": 3},
    ]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    result = dvc.plot("metric.json")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"y": 2, "x": 0, "rev": "workspace"},
            {"y": 3, "x": 1, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_confusion(tmp_dir, dvc):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    _write_json(tmp_dir, confusion_matrix, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    result = dvc.plot(datafile="metric.json", template="confusion")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"predicted": "B", "actual": "A", "rev": "workspace"},
            {"predicted": "A", "actual": "A", "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_multiple_revs(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.vt"), "template.vt"
    )

    metric_1 = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    _write_json(tmp_dir, metric_1, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    metric_2 = [{"x": 1, "y": 3}, {"x": 2, "y": 5}]
    _write_json(tmp_dir, metric_2, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "second", "v2")

    metric_3 = [{"x": 1, "y": 5}, {"x": 2, "y": 6}]
    _write_json(tmp_dir, metric_3, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "third")

    result = dvc.plot(
        "metric.json",
        template="template.vt",
        revisions=["HEAD", "v2", "v1"],
        file="result.html",
    )

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"y": 5, "x": 1, "rev": "HEAD"},
            {"y": 6, "x": 2, "rev": "HEAD"},
            {"y": 3, "x": 1, "rev": "v2"},
            {"y": 5, "x": 2, "rev": "v2"},
            {"y": 2, "x": 1, "rev": "v1"},
            {"y": 3, "x": 2, "rev": "v1"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_even_if_metric_missing(tmp_dir, scm, dvc, caplog):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    metric = [{"y": 2}, {"y": 3}]
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
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_throw_on_no_metric_at_all(tmp_dir, scm, dvc, caplog):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    tmp_dir.gen("some_file", "make repo dirty")

    caplog.clear()
    with pytest.raises(NoMetricInHistoryError) as error, caplog.at_level(
        logging.WARNING, "dvc"
    ):
        dvc.plot("metric.json", revisions=["v1"])

    # do not warn if none found
    assert len(caplog.messages) == 0

    assert (
        str(error.value)
        == "Could not find 'metric.json' on any of the revisions 'v1'"
    )


def test_custom_template(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.vt"),
        fspath(tmp_dir / "newtemplate.vt"),
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    result = dvc.plot("metric.json", "newtemplate.vt")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"a": 1, "b": 2, "rev": "workspace"},
            {"a": 2, "b": 3, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def _replace(path, src, dst):
    path.write_text(path.read_text().replace(src, dst))


def test_custom_template_with_specified_data(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.vt"),
        fspath(tmp_dir / "newtemplate.vt"),
    )
    _replace(
        tmp_dir / "newtemplate.vt",
        "DVC_METRIC_DATA",
        "DVC_METRIC_DATA,metric.json",
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    result = dvc.plot(datafile=None, template="newtemplate.vt")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"a": 1, "b": 2, "rev": "workspace"},
            {"a": 2, "b": 3, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_override_specified_data_source(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.vt"),
        fspath(tmp_dir / "newtemplate.vt"),
    )
    _replace(
        tmp_dir / "newtemplate.vt",
        "DVC_METRIC_DATA",
        "DVC_METRIC_DATA,metric.json",
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric2.json")
    _run_with_metric(tmp_dir, "metric2.json", "init", "v1")

    result = dvc.plot(datafile="metric2.json", template="newtemplate.vt")

    page_content = BeautifulSoup((tmp_dir / result).read_text())
    vega_data = json.dumps(
        [
            {"a": 1, "b": 2, "rev": "workspace"},
            {"a": 2, "b": 3, "rev": "workspace"},
        ],
        sort_keys=True,
    )
    assert _remove_whitespace(vega_data) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_should_embed_vega_json_template(tmp_dir, scm, dvc):
    template = DefaultLinearTemplate.DEFAULT_CONTENT
    template["data"] = {"values": "<DVC_METRIC_DATA>"}

    (tmp_dir / "template.vt").write_text(json.dumps(template))

    metric = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    result = dvc.plot("metric.json", "template.vt")

    result_content = json.loads((tmp_dir / result).read_text())
    vega_data = [
        {"x": 1, "y": 2, "rev": "workspace"},
        {"x": 2, "y": 3, "rev": "workspace"},
    ]

    assert vega_data == result_content["data"]["values"]


def test_should_raise_on_no_template_and_datafile(tmp_dir, dvc):
    with pytest.raises(NoDataOrTemplateProvided):
        dvc.plot()


def test_should_raise_on_no_template(tmp_dir, dvc):
    with pytest.raises(TemplateNotFound):
        dvc.plot("metric.json", "non_existing_template.vt")


def test_plot_no_data(tmp_dir, dvc):
    with pytest.raises(NoDataForTemplateError):
        dvc.plot(template="default")


def test_plot_wrong_metric_type(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("metric.txt", "content", commit="initial")
    with pytest.raises(PlotMetricTypeError):
        dvc.plot(datafile="metric.txt")
