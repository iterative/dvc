import json
import os
from collections import OrderedDict

import pytest
from funcy import get_in

from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import OverlappingOutputPathsError
from dvc.main import main
from dvc.path_info import PathInfo
from dvc.repo.plots.data import (
    INDEX_FIELD,
    REVISION_FIELD,
    PlotMetricTypeError,
)
from dvc.repo.plots.render import VegaRenderer
from dvc.repo.plots.template import (
    BadTemplateError,
    NoFieldInDataError,
    TemplateNotFoundError,
)
from dvc.utils import onerror_collect, relpath
from dvc.utils.fs import remove
from dvc.utils.serialize import (
    EncodingError,
    YAMLFileCorruptedError,
    dump_yaml,
    modify_yaml,
)
from tests.func.plots.utils import _write_csv


# RENDER
def test_plot_csv_one_column(tmp_dir, scm, dvc, run_copy_metrics):
    # no header
    props = {
        "header": False,
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


def test_plot_csv_multiple_columns(tmp_dir, scm, dvc, run_copy_metrics):
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


def test_plot_csv_choose_axes(tmp_dir, scm, dvc, run_copy_metrics):
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


def test_plot_confusion(tmp_dir, dvc, run_copy_metrics):
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


def test_plot_confusion_normalized(tmp_dir, dvc, run_copy_metrics):
    confusion_matrix = [
        {"predicted": "B", "actual": "A"},
        {"predicted": "A", "actual": "A"},
    ]

    props = {
        "template": "confusion_normalized",
        "x": "predicted",
        "y": "actual",
    }

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
    assert plot_content["spec"]["transform"][1]["groupby"] == [
        REVISION_FIELD,
        "actual",
    ]
    assert plot_content["spec"]["encoding"]["x"]["field"] == "predicted"
    assert plot_content["spec"]["encoding"]["y"]["field"] == "actual"


def test_plot_multiple_revs_default(tmp_dir, scm, dvc, run_copy_metrics):
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


# TODO add tests for grouping
def test_plot_even_if_metric_missing(
    tmp_dir, scm, dvc, caplog, run_copy_metrics
):

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


# TODO to collect?
# def test_plot_cache_missing(tmp_dir, scm, dvc, caplog, run_copy_metrics):
#     metric = [{"y": 2}, {"y": 3}]
#     _write_json(tmp_dir, metric, "metric_t.json")
#     stage = run_copy_metrics(
#         "metric_t.json",
#         "metric.json",
#         plots=["metric.json"],
#         commit="there is metric",
#     )
#     scm.tag("v1")
#
#     Make a different plot and then remove its datafile
# metric = [{"y": 3}, {"y": 4}]
# _write_json(tmp_dir, metric, "metric_t.json")
# stage = run_copy_metrics(
#     "metric_t.json",
#     "metric.json",
#     plots=["metric.json"],
#     commit="there is an another metric",
# )
# scm.tag("v2")
# remove(stage.outs[0].fspath)
# remove(stage.outs[0].cache_path)
#
# plots = dvc.plots.show(revs=["v1", "v2"], targets=["metric.json"])
# plot_content = json.loads(plots["metric.json"])
# assert plot_content["data"]["values"] == [
#     {"y": 2, INDEX_FIELD: 0, REVISION_FIELD: "v1"},
#     {"y": 3, INDEX_FIELD: 1, REVISION_FIELD: "v1"},
# ]


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


def test_should_raise_on_no_template(tmp_dir, dvc, run_copy_metrics):
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


def test_plot_wrong_metric_type(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.gen("metric_t.txt", "some text")
    run_copy_metrics(
        "metric_t.txt",
        "metric.txt",
        plots_no_cache=["metric.txt"],
        commit="add text metric",
    )

    assert isinstance(
        dvc.plots.collect(targets=["metric.txt"], onerror=onerror_collect)[
            "workspace"
        ]["data"]["metric.txt"]["error"],
        PlotMetricTypeError,
    )


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


# TODO ??
# def test_plot_default_choose_column(tmp_dir, scm, dvc, run_copy_metrics):
#     metric = [{"a": 1, "b": 2, "c": 3}, {"a": 2, "b": 3, "c": 4}]
#     _write_json(tmp_dir, metric, "metric_t.json")
#     run_copy_metrics(
#         "metric_t.json",
#         "metric.json",
#         plots_no_cache=["metric.json"],
#         commit="init",
#         tag="v1",
#     )
#
#     plot_string = dvc.plots.show(props={"fields": {"b"}})["metric.json"]
#
#     plot_content = json.loads(plot_string)
#     assert plot_content["data"]["values"] == [
#         {INDEX_FIELD: 0, "b": 2, REVISION_FIELD: "workspace"},
#         {INDEX_FIELD: 1, "b": 3, REVISION_FIELD: "workspace"},
#     ]
#     assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
#     assert plot_content["encoding"]["y"]["field"] == "b"
#


def test_raise_on_wrong_field(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"val": 2}, {"val": 3}]
    data = {
        "workspace": {
            "data": {"file.json": {"data": metric, "props": {"x": "no_val"}}}
        }
    }

    with pytest.raises(NoFieldInDataError):
        VegaRenderer(data, dvc.plots.templates).get_vega()


# TODO move to collect?
# @pytest.mark.parametrize("use_dvc", [True, False])
# def test_show_non_plot(tmp_dir, scm, use_dvc):
#     metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
#     _write_json(tmp_dir, metric, "metric.json")
#
#     if use_dvc:
#         dvc = Repo.init()
#     else:
#         dvc = Repo(uninitialized=True)
#
#     plot_string = dvc.plots.show(targets=["metric.json"])["metric.json"]
#
#     plot_content = json.loads(plot_string)
#     assert plot_content["data"]["values"] == [
#         {
#             "val": 2,
#             INDEX_FIELD: 0,
#             "first_val": 100,
#             REVISION_FIELD: "workspace",
#         },
#         {
#             "val": 3,
#             INDEX_FIELD: 1,
#             "first_val": 200,
#             REVISION_FIELD: "workspace",
#         },
#     ]
#     assert plot_content["encoding"]["x"]["field"] == INDEX_FIELD
#     assert plot_content["encoding"]["y"]["field"] == "val"
#
#     if not use_dvc:
#         assert not (tmp_dir / ".dvc").exists()
#

# TODO?
# def test_show_non_plot_and_plot_with_params(
#     tmp_dir, scm, dvc, run_copy_metrics
# ):
#     metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
#     _write_json(tmp_dir, metric, "metric.json")
#     run_copy_metrics(
#         "metric.json", "metric2.json", plots_no_cache=["metric2.json"]
#     )

# dvc.plots.modify("metric2.json", props={"title": "TITLE"})
# result = dvc.plots.show(targets=["metric.json", "metric2.json"])

# plot_content = json.loads(result["metric.json"])
# plot2_content = json.loads(result["metric2.json"])

# assert plot2_content["title"] == "TITLE"

# assert plot_content != plot2_content
# plot_content.pop("title")
# plot2_content.pop("title")
# assert plot_content == plot2_content

# TODO?
# def test_show_no_repo(tmp_dir):
#     metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
#     _write_json(tmp_dir, metric, "metric.json")

# dvc = Repo(uninitialized=True)

# dvc.plots.show(["metric.json"])


# TODO?
# def test_show_from_subdir(tmp_dir, dvc, capsys):
#     subdir = tmp_dir / "subdir"

# subdir.mkdir()
# metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
# _write_json(subdir, metric, "metric.json")

# with subdir.chdir():
#     assert main(["plots", "show", "metric.json"]) == 0

# out, _ = capsys.readouterr()
# assert subdir.as_uri() in out
# assert (subdir / "dvc_plots").is_dir()
# assert (subdir / "dvc_plots" / "index.html").is_file()

# TODO collect format
# def test_plots_show_non_existing(tmp_dir, dvc):
#     assert dvc.plots.show(targets=["plot.json"]) == {}


@pytest.mark.parametrize("clear_before_run", [True, False])
def test_plots_show_overlap(tmp_dir, dvc, run_copy_metrics, clear_before_run):
    data_dir = PathInfo("data")
    (tmp_dir / data_dir).mkdir()

    dump_yaml(data_dir / "m1_temp.yaml", {"a": {"b": {"c": 2, "d": 1}}})
    run_copy_metrics(
        str(data_dir / "m1_temp.yaml"),
        str(data_dir / "m1.yaml"),
        single_stage=False,
        commit="add m1",
        name="cp-m1",
        plots=[str(data_dir / "m1.yaml")],
    )
    with modify_yaml("dvc.yaml") as d:
        # trying to make an output overlaps error
        d["stages"]["corrupted-stage"] = {
            "cmd": "mkdir data",
            "outs": ["data"],
        }

    # running by clearing and not clearing stuffs
    # so as it works even for optimized cases
    if clear_before_run:
        remove(data_dir)
        remove(dvc.odb.local.cache_dir)

    dvc._reset()

    assert isinstance(
        dvc.plots.collect(onerror=onerror_collect)["workspace"]["error"],
        OverlappingOutputPathsError,
    )


# TODO
# def test_dir_plots(tmp_dir, dvc, run_copy_metrics):
#     subdir = tmp_dir / "subdir"
#     subdir.mkdir()
#
#     metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
#
#     fname = "file.json"
#     _write_json(tmp_dir, metric, fname)
#
#     p1 = os.path.join("subdir", "p1.json")
#     p2 = os.path.join("subdir", "p2.json")
#     tmp_dir.dvc.run(
#         cmd=(
#             f"mkdir subdir && python copy.py {fname} {p1} && "
#             f"python copy.py {fname} {p2}"
#         ),
#         deps=[fname],
#         single_stage=False,
#         plots=["subdir"],
#         name="copy_double",
#     )
#     dvc.plots.modify("subdir", {"title": "TITLE"})
#
#     result = dvc.plots.show()
#     p1_content = json.loads(result[p1])
#     p2_content = json.loads(result[p2])
#
#     assert p1_content["title"] == p2_content["title"] == "TITLE"


# TODO?
# def test_show_dir_plots(tmp_dir, dvc, run_copy_metrics):
#     subdir = tmp_dir / "subdir"
#     subdir.mkdir()
#     metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
#
#     fname = "file.json"
#     _write_json(tmp_dir, metric, fname)
#
#     p1 = os.path.join("subdir", "p1.json")
#     p2 = os.path.join("subdir", "p2.json")
#     tmp_dir.dvc.run(
#         cmd=(
#             f"mkdir subdir && python copy.py {fname} {p1} && "
#             f"python copy.py {fname} {p2}"
#         ),
#         deps=[fname],
#         single_stage=False,
#         plots=["subdir"],
#         name="copy_double",
#     )
#
#     result = dvc.plots.show(targets=["subdir"])
#     p1_content = json.loads(result[p1])
#     p2_content = json.loads(result[p2])
#
#     assert p1_content == p2_content
#
#     result = dvc.plots.show(targets=[p1])
#     assert set(result.keys()) == {p1}
#
#     remove(dvc.odb.local.cache_dir)
#     remove(subdir)
#
#     assert dvc.plots.show() == {}


def test_ignore_binary_file(tmp_dir, dvc, run_copy_metrics):
    with open("file", "wb") as fobj:
        fobj.write(b"\xc1")

    run_copy_metrics("file", "plot_file.json", plots=["plot_file.json"])
    result = dvc.plots.collect(onerror=onerror_collect)

    assert isinstance(
        result["workspace"]["data"]["plot_file.json"]["error"], EncodingError
    )


@pytest.mark.parametrize(
    "file,error_path",
    (
        (PIPELINE_FILE, ["workspace", "error"]),
        ("plot.yaml", ["workspace", "data", "plot.yaml", "error"]),
    ),
)
def test_log_errors(
    tmp_dir, scm, dvc, run_copy_metrics, file, error_path, capsys
):
    metric = [{"val": 2}, {"val": 3}]
    dump_yaml("metric_t.yaml", metric)
    run_copy_metrics(
        "metric_t.yaml",
        "plot.yaml",
        plots=["plot.yaml"],
        single_stage=False,
        name="train",
    )
    scm.tag("v1")

    with open(file, "a") as fd:
        fd.write("\nMALFORMED!")

    result = dvc.plots.collect(onerror=onerror_collect)
    _, error = capsys.readouterr()

    assert isinstance(get_in(result, error_path), YAMLFileCorruptedError)
    assert (
        "DVC failed to load some plots for following revisions: 'workspace'."
        in error
    )


def test_plots_binary(tmp_dir, scm, dvc, run_copy_metrics, custom_template):
    with open("image.jpg", "wb") as fd:
        fd.write(b"content")

    metric = [{"val": 2}, {"val": 3}]
    _write_csv(metric, "metric_t.csv")

    dvc.add(["image.jpg", "metric_t.csv"])
    run_copy_metrics(
        "metric_t.csv",
        "metric.csv",
        plots=["metric.csv"],
        name="s1",
        single_stage=False,
    )
    run_copy_metrics(
        "image.jpg",
        "plot.jpg",
        commit="run training",
        plots=["plot.jpg"],
        name="s2",
        single_stage=False,
    )
    dvc.plots.modify(
        "metric.csv", props={"template": relpath(custom_template)}
    )
    scm.add(["dvc.yaml", "dvc.lock"])
    scm.commit("initial")

    scm.tag("v1")

    with open("plot.jpg", "wb") as fd:
        fd.write(b"content2")

    _write_csv([{"val": 3}, {"val": 4}], "metric.csv")

    # dvc.plots.show(revs=["v1", "workspace"])
    main(["plots", "diff", "v1"])
