import json
import os
from collections import OrderedDict

import pytest

from dvc.repo.plots.data import INDEX_FIELD, REVISION_FIELD
from dvc.repo.plots.render import VegaRenderer
from dvc.repo.plots.template import (
    BadTemplateError,
    NoFieldInDataError,
    TemplateNotFoundError,
)


def test_one_column(tmp_dir, scm, dvc, run_copy_metrics):
    props = {
        "x_label": "x_title",
        "y_label": "y_title",
        "title": "mytitle",
    }
    data = {
        "workspace": {
            "data": {
                "file.json": {"data": [{"val": 2}, {"val": 3}], "props": props}
            }
        }
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["title"] == "mytitle"
    assert plot_content["data"]["values"] == [
        {"val": 2, INDEX_FIELD: 0, REVISION_FIELD: "workspace"},
        {"val": 3, INDEX_FIELD: 1, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"
    assert plot_content["encoding"]["x"]["title"] == "x_title"
    assert plot_content["encoding"]["y"]["title"] == "y_title"


def test_multiple_columns(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]

    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": {}}}}
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": 2,
            INDEX_FIELD: 0,
            REVISION_FIELD: "workspace",
            "first_val": 100,
            "second_val": 100,
        },
        {
            "val": 3,
            INDEX_FIELD: 1,
            REVISION_FIELD: "workspace",
            "first_val": 200,
            "second_val": 300,
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "val"


def test_choose_axes(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]

    props = {"x": "first_val", "y": "second_val"}

    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }
    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {
            "val": 2,
            REVISION_FIELD: "workspace",
            "first_val": 100,
            "second_val": 100,
        },
        {
            "val": 3,
            REVISION_FIELD: "workspace",
            "first_val": 200,
            "second_val": 300,
        },
    ]
    assert plot_content["encoding"]["x"]["field"] == "first_val"
    assert plot_content["encoding"]["y"]["field"] == "second_val"


def test_confusion(tmp_dir, dvc, run_copy_metrics):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    props = {"template": "confusion", "x": "predicted", "y": "actual"}

    data = {
        "workspace": {
            "data": {"file.json": {"data": confusion_matrix, "props": props}}
        }
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"predicted": "B", "actual": "A", REVISION_FIELD: "workspace"},
        {"predicted": "A", "actual": "A", REVISION_FIELD: "workspace"},
    ]
    assert plot_content["spec"]["transform"][0]["groupby"] == [
        "actual",
        "predicted",
    ]
    assert plot_content["spec"]["encoding"]["x"]["field"] == "predicted"
    assert plot_content["spec"]["encoding"]["y"]["field"] == "actual"


def test_multiple_revs_default(tmp_dir, scm, dvc, run_copy_metrics):
    metric_1 = [{"y": 2}, {"y": 3}]
    metric_2 = [{"y": 3}, {"y": 5}]
    metric_3 = [{"y": 5}, {"y": 6}]

    data = {
        "HEAD": {
            "data": {
                "file.json": {"data": metric_3, "props": {"fields": {"y"}}}
            }
        },
        "v2": {
            "data": {
                "file.json": {"data": metric_2, "props": {"fields": {"y"}}}
            }
        },
        "v1": {
            "data": {
                "file.json": {"data": metric_1, "props": {"fields": {"y"}}}
            }
        },
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 5, INDEX_FIELD: 0, REVISION_FIELD: "HEAD"},
        {"y": 6, INDEX_FIELD: 1, REVISION_FIELD: "HEAD"},
        {"y": 3, INDEX_FIELD: 0, REVISION_FIELD: "v2"},
        {"y": 5, INDEX_FIELD: 1, REVISION_FIELD: "v2"},
        {"y": 2, INDEX_FIELD: 0, REVISION_FIELD: "v1"},
        {"y": 3, INDEX_FIELD: 1, REVISION_FIELD: "v1"},
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_metric_missing(tmp_dir, scm, dvc, caplog, run_copy_metrics):

    metric = [{"y": 2}, {"y": 3}]
    data = {
        "v2": {"data": {"file.json": {"data": metric, "props": {}}}},
        "workspace": {
            "data": {"file.json": {"error": FileNotFoundError(), "props": {}}}
        },
    }
    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"y": 2, INDEX_FIELD: 0, REVISION_FIELD: "v2"},
        {"y": 3, INDEX_FIELD: 1, REVISION_FIELD: "v2"},
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "y"


def test_custom_template(tmp_dir, scm, dvc, custom_template):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    props = {"template": os.fspath(custom_template), "x": "a", "y": "b"}
    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"a": 1, "b": 2, REVISION_FIELD: "workspace"},
        {"a": 2, "b": 3, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "a"
    assert plot_content["encoding"]["y"]["field"] == "b"


def test_raise_on_no_template(tmp_dir, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    props = {"template": "non_existing_template.json"}
    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }

    with pytest.raises(TemplateNotFoundError):
        VegaRenderer(data, dvc.plots.templates).get_vega()


def test_bad_template(tmp_dir, dvc):
    metric = [{"val": 2}, {"val": 3}]
    tmp_dir.gen("template.json", json.dumps({"a": "b", "c": "d"}))
    props = {"template": "template.json"}
    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }

    with pytest.raises(BadTemplateError):
        VegaRenderer(data, dvc.plots.templates).get_vega()


def test_plot_choose_columns(
    tmp_dir, scm, dvc, custom_template, run_copy_metrics
):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    props = {
        "template": os.fspath(custom_template),
        "fields": {"b", "c"},
        "x": "b",
        "y": "c",
    }
    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()

    plot_content = json.loads(plot_string)
    assert plot_content["data"]["values"] == [
        {"b": 2, "c": 3, REVISION_FIELD: "workspace"},
        {"b": 3, "c": 4, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "b"
    assert plot_content["encoding"]["y"]["field"] == "c"


def test_plot_default_choose_column(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    data = {
        "workspace": {
            "data": {"file.json": {"data": metric, "props": {"fields": {"b"}}}}
        }
    }

    plot_string = VegaRenderer(data, dvc.plots.templates).get_vega()
    plot_content = json.loads(plot_string)

    assert plot_content["data"]["values"] == [
        {INDEX_FIELD: 0, "b": 2, REVISION_FIELD: "workspace"},
        {INDEX_FIELD: 1, "b": 3, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
    assert plot_content["encoding"]["y"]["field"] == "b"


def test_raise_on_wrong_field(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    data = {
        "workspace": {
            "data": {"file.json": {"data": metric, "props": {"x": "no_val"}}}
        }
    }

    with pytest.raises(NoFieldInDataError):
        VegaRenderer(data, dvc.plots.templates).get_vega()
