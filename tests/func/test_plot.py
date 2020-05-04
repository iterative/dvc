import csv
import json
import logging
import shutil
from collections import OrderedDict

import pytest
import yaml
from bs4 import BeautifulSoup
from funcy import first

from dvc.compat import fspath
from dvc.repo.plot.data import (
    NoMetricInHistoryError,
    PlotMetricTypeError,
    PlotData,
)
from dvc.repo.plot.template import (
    TemplateNotFoundError,
    NoDataForTemplateError,
)
from dvc.repo.plot import NoDataOrTemplateProvided


def _remove_whitespace(value):
    return value.replace(" ", "").replace("\n", "")


def _run_with_metric(tmp_dir, metric_filename, commit=None, tag=None):
    tmp_dir.dvc.run(metrics_no_cache=[metric_filename], single_stage=True)
    if hasattr(tmp_dir.dvc, "scm"):
        tmp_dir.dvc.scm.add([metric_filename, metric_filename + ".dvc"])
        if commit:
            tmp_dir.dvc.scm.commit(commit)
        if tag:
            tmp_dir.dvc.scm.tag(tag)


def _write_csv(metric, filename, header=True):
    with open(filename, "w", newline="") as csvobj:
        if header:
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
    # no header
    metric = [{"val": 2}, {"val": 3}]
    _write_csv(metric, "metric.csv", header=False)
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    plot_string = dvc.plot(
        "metric.csv",
        csv_header=False,
        x_title="x_title",
        y_title="y_title",
        title="mytitle",
    )

    plot_content = json.loads(plot_string)
    assert plot_content["title"] == "mytitle"
    assert plot_content["data"]["values"] == [
        {"0": "2", PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"0": "3", PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "0"
    assert plot_content["encoding"]["x"]["title"] == "x_title"
    assert plot_content["encoding"]["y"]["title"] == "y_title"


def test_plot_csv_multiple_columns(tmp_dir, scm, dvc):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    _write_csv(metric, "metric.csv")
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    plot_string = dvc.plot("metric.csv")

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": "2",
            PlotData.INDEX_FIELD: 0,
            "rev": "workspace",
            "first_val": "100",
            "second_val": "100",
        },
        {
            "val": "3",
            PlotData.INDEX_FIELD: 1,
            "rev": "workspace",
            "first_val": "200",
            "second_val": "300",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"


def test_plot_csv_choose_axes(tmp_dir, scm, dvc):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]
    _write_csv(metric, "metric.csv")
    _run_with_metric(tmp_dir, metric_filename="metric.csv")

    plot_string = dvc.plot(
        "metric.csv", x_field="first_val", y_field="second_val"
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": "2",
            "rev": "workspace",
            "first_val": "100",
            "second_val": "100",
        },
        {
            "val": "3",
            "rev": "workspace",
            "first_val": "200",
            "second_val": "300",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == "first_val"
    assert plot_content["encoding"]["y"]["field"] == "second_val"


def test_plot_json_single_val(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    plot_string = dvc.plot("metric.json")

    plot_json = json.loads(plot_string)
    assert plot_json["data"]["values"] == [
        {"val": 2, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"val": 3, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
    assert plot_json["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_json["encoding"]["y"]["field"] == "val"


def test_plot_json_multiple_val(tmp_dir, scm, dvc):
    metric = [
        {"first_val": 100, "val": 2},
        {"first_val": 200, "val": 3},
    ]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    plot_string = dvc.plot("metric.json")

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": 2,
            PlotData.INDEX_FIELD: 0,
            "first_val": 100,
            "rev": "workspace",
        },
        {
            "val": 3,
            PlotData.INDEX_FIELD: 1,
            "first_val": 200,
            "rev": "workspace",
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"


def test_plot_confusion(tmp_dir, dvc):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    _write_json(tmp_dir, confusion_matrix, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    plot_string = dvc.plot(
        datafile="metric.json",
        template="confusion",
        x_field="predicted",
        y_field="actual",
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"predicted": "B", "actual": "A", "rev": "workspace"},
        {"predicted": "A", "actual": "A", "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "predicted"
    assert plot_content["encoding"]["y"]["field"] == "actual"


def test_plot_multiple_revs_default(tmp_dir, scm, dvc):
    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "second", "v2")

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "third")

    plot_string = dvc.plot(
        "metric.json", fields={"y"}, revisions=["HEAD", "v2", "v1"],
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 5, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 6, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "v2"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "v2"},
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v1"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v1"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_plot_multiple_revs(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.json"), "template.json"
    )

    metric_1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric_1, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    metric_2 = [{"y": 3}, {"y": 5}]
    _write_json(tmp_dir, metric_2, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "second", "v2")

    metric_3 = [{"y": 5}, {"y": 6}]
    _write_json(tmp_dir, metric_3, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "third")

    plot_string = dvc.plot(
        "metric.json",
        template="template.json",
        revisions=["HEAD", "v2", "v1"],
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 5, PlotData.INDEX_FIELD: 0, "rev": "HEAD"},
        {"y": 6, PlotData.INDEX_FIELD: 1, "rev": "HEAD"},
        {"y": 3, PlotData.INDEX_FIELD: 0, "rev": "v2"},
        {"y": 5, PlotData.INDEX_FIELD: 1, "rev": "v2"},
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v1"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v1"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_plot_even_if_metric_missing(tmp_dir, scm, dvc, caplog):
    tmp_dir.scm_gen("some_file", "content", commit="there is no metric")
    scm.tag("v1")

    metric = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "there is metric", "v2")

    caplog.clear()
    with caplog.at_level(logging.WARNING, "dvc"):
        plot_string = dvc.plot("metric.json", revisions=["v1", "v2"])
        assert (
            "File 'metric.json' was not found at: 'v1'. "
            "It will not be plotted." in caplog.text
        )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 2, PlotData.INDEX_FIELD: 0, "rev": "v2"},
        {"y": 3, PlotData.INDEX_FIELD: 1, "rev": "v2"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


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

    assert str(error.value) == "Could not find 'metric.json'."


@pytest.fixture()
def custom_template(tmp_dir, dvc):
    custom_template = tmp_dir / "custom_template.json"
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.json"),
        fspath(custom_template),
    )
    return custom_template


def test_custom_template(tmp_dir, scm, dvc, custom_template):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    plot_string = dvc.plot(
        "metric.json", fspath(custom_template), x_field="a", y_field="b"
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"a": 1, "b": 2, "rev": "workspace"},
        {"a": 2, "b": 3, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "a"
    assert plot_content["encoding"]["y"]["field"] == "b"


def _replace(path, src, dst):
    path.write_text(path.read_text().replace(src, dst))


def test_custom_template_with_specified_data(
    tmp_dir, scm, dvc, custom_template
):
    _replace(
        custom_template, "DVC_METRIC_DATA", "DVC_METRIC_DATA,metric.json",
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    plot_string = dvc.plot(
        datafile=None,
        template=fspath(custom_template),
        x_field="a",
        y_field="b",
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"a": 1, "b": 2, "rev": "workspace"},
        {"a": 2, "b": 3, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "a"
    assert plot_content["encoding"]["y"]["field"] == "b"


def test_plot_override_specified_data_source(tmp_dir, scm, dvc):
    shutil.copy(
        fspath(tmp_dir / ".dvc" / "plot" / "default.json"),
        fspath(tmp_dir / "newtemplate.json"),
    )
    _replace(
        tmp_dir / "newtemplate.json",
        "DVC_METRIC_DATA",
        "DVC_METRIC_DATA,metric.json",
    )

    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric2.json")
    _run_with_metric(tmp_dir, "metric2.json", "init", "v1")

    plot_string = dvc.plot(
        datafile="metric2.json", template="newtemplate.json", x_field="a"
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"a": 1, "b": 2, "rev": "workspace"},
        {"a": 2, "b": 3, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "a"
    assert plot_content["encoding"]["y"]["field"] == "b"


def test_should_raise_on_no_template_and_datafile(tmp_dir, dvc):
    with pytest.raises(NoDataOrTemplateProvided):
        dvc.plot()


def test_should_raise_on_no_template(tmp_dir, dvc):
    with pytest.raises(TemplateNotFoundError):
        dvc.plot("metric.json", "non_existing_template.json")


def test_plot_no_data(tmp_dir, dvc):
    with pytest.raises(NoDataForTemplateError):
        dvc.plot(template="default")


def test_plot_wrong_metric_type(tmp_dir, scm, dvc):
    tmp_dir.scm_gen("metric.txt", "content", commit="initial")
    with pytest.raises(PlotMetricTypeError):
        dvc.plot(datafile="metric.txt")


def test_plot_choose_columns(tmp_dir, scm, dvc, custom_template):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    plot_string = dvc.plot(
        "metric.json",
        fspath(custom_template),
        fields={"b", "c"},
        x_field="b",
        y_field="c",
    )

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"b": 2, "c": 3, "rev": "workspace"},
        {"b": 3, "c": 4, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "b"
    assert plot_content["encoding"]["y"]["field"] == "c"


def test_plot_default_choose_column(tmp_dir, scm, dvc):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "init", "v1")

    plot_string = dvc.plot("metric.json", fields={"b"})

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {PlotData.INDEX_FIELD: 0, "b": 2, "rev": "workspace"},
        {PlotData.INDEX_FIELD: 1, "b": 3, "rev": "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == PlotData.INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "b"


def test_plot_embed(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    _run_with_metric(tmp_dir, "metric.json", "first run")

    plot_string = dvc.plot("metric.json", embed=True, y_field="val")

    page_content = BeautifulSoup(plot_string)
    data_dump = json.dumps(
        [
            {"val": 2, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
            {"val": 3, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
        ],
        sort_keys=True,
    )

    assert _remove_whitespace(data_dump) in _remove_whitespace(
        first(page_content.body.script.contents)
    )


def test_plot_yaml(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    with open("metric.yaml", "w") as fobj:
        yaml.dump(metric, fobj)

    _run_with_metric(tmp_dir, metric_filename="metric.yaml")

    plot_string = dvc.plot("metric.yaml",)

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"val": 2, PlotData.INDEX_FIELD: 0, "rev": "workspace"},
        {"val": 3, PlotData.INDEX_FIELD: 1, "rev": "workspace"},
    ]
