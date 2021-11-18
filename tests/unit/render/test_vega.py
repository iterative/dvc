import json
import os
from collections import OrderedDict

import pytest
from funcy import first

from dvc.render.base import BadTemplateError
from dvc.render.data import INDEX_FIELD, REVISION_FIELD
from dvc.render.utils import group_by_filename
from dvc.render.vega import VegaRenderer
from dvc.repo.plots.template import NoFieldInDataError


def test_group_plots_data():
    error = FileNotFoundError()
    data = {
        "v2": {
            "data": {
                "file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}},
                "other_file.jpg": {"data": "content"},
            }
        },
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
                "other_file.jpg": {"data": "content2"},
            }
        },
    }

    results = group_by_filename(data)
    assert {
        "v2": {
            "data": {
                "file.json": {"data": [{"y": 2}, {"y": 3}], "props": {}},
            }
        },
        "v1": {
            "data": {"file.json": {"data": [{"y": 4}, {"y": 5}], "props": {}}}
        },
        "workspace": {
            "data": {
                "file.json": {"error": error, "props": {}},
            }
        },
    } in results
    assert {
        "v2": {
            "data": {
                "other_file.jpg": {"data": "content"},
            }
        },
        "workspace": {
            "data": {
                "other_file.jpg": {"data": "content2"},
            }
        },
    } in results


def test_one_column(tmp_dir, dvc):
    props = {
        "x_label": "x_title",
        "y_label": "y_title",
        "title": "mytitle",
    }
    data = {
        "workspace": {
            "data": {"file.json": {"data": [{"val": 2}, {"val": 3}]}}
        }
    }

    plot_content = VegaRenderer(
        data, template=dvc.plots.templates.load(), properties=props
    ).asdict()

    assert plot_content["title"] == "mytitle"
    assert plot_content["data"]["values"] == [
        {"val": 2, INDEX_FIELD: 0, REVISION_FIELD: "workspace"},
        {"val": 3, INDEX_FIELD: 1, REVISION_FIELD: "workspace"},
    ]
    assert (
        first(plot_content["layer"])["encoding"]["x"]["field"] == INDEX_FIELD
    )
    assert first(plot_content["layer"])["encoding"]["y"]["field"] == "val"
    assert first(plot_content["layer"])["encoding"]["x"]["title"] == "x_title"
    assert first(plot_content["layer"])["encoding"]["y"]["title"] == "y_title"


def test_multiple_columns(tmp_dir, scm, dvc):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]

    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": {}}}}
    }

    plot_content = VegaRenderer(data, dvc.plots.templates.load()).asdict()

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
    assert (
        first(plot_content["layer"])["encoding"]["x"]["field"] == INDEX_FIELD
    )
    assert first(plot_content["layer"])["encoding"]["y"]["field"] == "val"


def test_choose_axes(tmp_dir, scm, dvc):
    metric = [
        OrderedDict([("first_val", 100), ("second_val", 100), ("val", 2)]),
        OrderedDict([("first_val", 200), ("second_val", 300), ("val", 3)]),
    ]

    props = {"x": "first_val", "y": "second_val"}

    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }
    plot_content = VegaRenderer(
        data, template=dvc.plots.templates.load(), properties=props
    ).asdict()

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
    assert (
        first(plot_content["layer"])["encoding"]["x"]["field"] == "first_val"
    )
    assert (
        first(plot_content["layer"])["encoding"]["y"]["field"] == "second_val"
    )


def test_confusion(tmp_dir, dvc):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]
    props = {"template": "confusion", "x": "predicted", "y": "actual"}

    data = {"workspace": {"data": {"file.json": {"data": confusion_matrix}}}}

    plot_content = VegaRenderer(
        data, template=dvc.plots.templates.load("confusion"), properties=props
    ).asdict()

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


def test_multiple_revs_default(tmp_dir, scm, dvc):
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

    plot_content = VegaRenderer(data, dvc.plots.templates.load()).asdict()

    assert plot_content["data"]["values"] == [
        {"y": 5, INDEX_FIELD: 0, REVISION_FIELD: "HEAD"},
        {"y": 6, INDEX_FIELD: 1, REVISION_FIELD: "HEAD"},
        {"y": 3, INDEX_FIELD: 0, REVISION_FIELD: "v2"},
        {"y": 5, INDEX_FIELD: 1, REVISION_FIELD: "v2"},
        {"y": 2, INDEX_FIELD: 0, REVISION_FIELD: "v1"},
        {"y": 3, INDEX_FIELD: 1, REVISION_FIELD: "v1"},
    ]
    assert (
        first(plot_content["layer"])["encoding"]["x"]["field"] == INDEX_FIELD
    )
    assert first(plot_content["layer"])["encoding"]["y"]["field"] == "y"


def test_metric_missing(tmp_dir, scm, dvc, caplog):

    metric = [{"y": 2}, {"y": 3}]
    data = {
        "v2": {"data": {"file.json": {"data": metric, "props": {}}}},
        "workspace": {
            "data": {"file.json": {"error": FileNotFoundError(), "props": {}}}
        },
    }
    plot_content = VegaRenderer(data, dvc.plots.templates.load()).asdict()

    assert plot_content["data"]["values"] == [
        {"y": 2, INDEX_FIELD: 0, REVISION_FIELD: "v2"},
        {"y": 3, INDEX_FIELD: 1, REVISION_FIELD: "v2"},
    ]
    assert (
        first(plot_content["layer"])["encoding"]["x"]["field"] == INDEX_FIELD
    )
    assert first(plot_content["layer"])["encoding"]["y"]["field"] == "y"


def test_bad_template(tmp_dir, dvc):
    metric = [{"val": 2}, {"val": 3}]
    data = {"workspace": {"data": {"file.json": {"data": metric}}}}

    from dvc.repo.plots.template import Template

    with pytest.raises(BadTemplateError):
        VegaRenderer(data, Template("name", "content")).asdict()


def test_plot_choose_columns(tmp_dir, scm, dvc, custom_template):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    props = {
        "fields": {"b", "c"},
        "x": "b",
        "y": "c",
    }
    data = {
        "workspace": {"data": {"file.json": {"data": metric, "props": props}}}
    }

    plot_content = VegaRenderer(
        data,
        template=dvc.plots.templates.load(os.fspath(custom_template)),
        properties=props,
    ).asdict()

    assert plot_content["data"]["values"] == [
        {"b": 2, "c": 3, REVISION_FIELD: "workspace"},
        {"b": 3, "c": 4, REVISION_FIELD: "workspace"},
    ]
    assert plot_content["encoding"]["x"]["field"] == "b"
    assert plot_content["encoding"]["y"]["field"] == "c"


def test_raise_on_wrong_field(tmp_dir, scm, dvc):
    metric = [{"val": 2}, {"val": 3}]
    props = {"x": "no_val"}
    data = {"workspace": {"data": {"file.json": {"data": metric}}}}

    with pytest.raises(NoFieldInDataError):
        VegaRenderer(
            data, template=dvc.plots.templates.load(), properties=props
        ).asdict()


@pytest.mark.parametrize(
    "extension, matches",
    (
        (".csv", True),
        (".json", True),
        (".tsv", True),
        (".yaml", True),
        (".jpg", False),
        (".gif", False),
        (".jpeg", False),
        (".png", False),
    ),
)
def test_matches(extension, matches):
    filename = "file" + extension
    data = {
        "HEAD": {"data": {filename: {}}},
        "v1": {"data": {filename: {}}},
    }
    assert VegaRenderer.matches(data) == matches


@pytest.mark.parametrize(
    "template_path, target_name",
    [
        (os.path.join(".dvc", "plots", "template.json"), "template"),
        (os.path.join(".dvc", "plots", "template.json"), "template.json"),
        (
            os.path.join(".dvc", "plots", "subdir", "template.json"),
            os.path.join("subdir", "template.json"),
        ),
        (
            os.path.join(".dvc", "plots", "subdir", "template.json"),
            os.path.join("subdir", "template"),
        ),
        ("template.json", "template.json"),
    ],
)
def test_should_resolve_template(tmp_dir, dvc, template_path, target_name):
    os.makedirs(os.path.abspath(os.path.dirname(template_path)), exist_ok=True)
    with open(template_path, "w", encoding="utf-8") as fd:
        fd.write("template_content")

    assert dvc.plots.templates.get_template(target_name) == os.path.abspath(
        template_path
    )


def test_as_json(tmp_dir, scm, dvc):
    metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
    data = {"workspace": {"data": {"file.json": {"data": metric}}}}
    props = {"fields": {"b"}}

    renderer = VegaRenderer(
        data, template=dvc.plots.templates.load(), properties=props
    )
    plot_content = renderer.asdict()
    plot_as_json = first(json.loads(renderer.as_json()))

    assert plot_as_json["type"] == "vega"
    assert plot_as_json["revisions"] == ["workspace"]
    assert plot_as_json["content"] == plot_content
