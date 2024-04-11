import json
import os
from copy import deepcopy
from urllib.parse import urlparse
from urllib.request import url2pathname

import dpath
import pytest
from bs4 import BeautifulSoup
from funcy import first

from dvc.cli import main
from dvc.render import ANCHOR_DEFINITIONS, FILENAME, REVISION

JSON_OUT = "vis_data"


def call(capsys, subcommand="show"):
    capsys.readouterr()
    assert main(["plots", subcommand, "--json", "-o", JSON_OUT, "--split"]) == 0
    split_json_out, _ = capsys.readouterr()

    split_json_result = json.loads(split_json_out)

    capsys.readouterr()
    assert main(["plots", subcommand, "--json", "-o", JSON_OUT]) == 0
    json_out, _ = capsys.readouterr()

    json_result = json.loads(json_out)

    assert main(["plots", subcommand]) == 0
    html_path_out, _ = capsys.readouterr()

    parsed = urlparse(html_path_out.strip())
    abspath = url2pathname(parsed.path)
    return abspath, json_result, split_json_result


def extract_vega_specs(html_path, plots_ids):
    from dvc_render.base import Renderer

    result = {}

    with open(html_path, encoding="utf-8") as fd:
        content = fd.read()

    reader = BeautifulSoup(content, features="html.parser")
    for plot_id in plots_ids:
        script = _remove_blanks(
            reader.find("div", id=Renderer.remove_special_chars(plot_id)).script.text
        )
        result[plot_id] = json.loads(
            script.split("; vegaEmbed")[0].replace("var spec = ", "")
        )

    return result


def drop_fields(datapoints: list[dict], fields: list[str]):
    tmp = deepcopy(datapoints)
    for datapoint in tmp:
        keys = set(datapoint.keys())
        for key in keys:
            if key in fields:
                datapoint.pop(key)
    return tmp


def verify_image(path, version, filename, content, html_path, json_result):
    assert os.path.exists(html_path)
    with open(html_path, encoding="utf-8") as fd:
        html_content = fd.read()

    image_data = {}
    for datapoint in json_result[filename]:
        if datapoint["revisions"] == [version]:
            image_data = datapoint
            break

    assert image_data, f"{version} data for {filename} was not found"
    assert image_data["type"] == "image"
    output_filename = filename.replace("/", "_")
    output_name = f"{version}_{output_filename}"
    assert image_data["url"] == str(path / JSON_OUT / output_name)
    assert (path / JSON_OUT / output_name).read_bytes() == content

    assert os.path.join("static", output_name) in html_content

    # there should be no absolute paths in produced HTML
    # TODO uncomment once dvc-render is adjusted
    # assert str(path) not in html_content
    assert (path / "dvc_plots" / "static" / output_name).read_bytes() == content


def _remove_blanks(text: str):
    return " ".join(text.replace("\t", "").replace("\n", "").split())


def verify_vega(
    versions, html_result, json_result, split_json_result, title, x_label, y_label
):
    if isinstance(versions, str):
        versions = [versions]

    for j in [json_result, split_json_result]:
        assert len(j) == 1
        assert j[0]["type"] == "vega"
        assert set(j[0]["revisions"]) == set(versions)

    assert (
        json_result[0]["content"]["data"]["values"]
        == split_json_result[0][ANCHOR_DEFINITIONS]["<DVC_METRIC_DATA>"]
    )

    assert set(versions) == set(json_result[0]["revisions"])

    assert json_result[0]["content"]["data"]["values"]
    assert html_result["data"]["values"]

    content_str = json.dumps(split_json_result[0]["content"])
    assert "<DVC_METRIC_DATA>" in content_str
    assert "<DVC_METRIC_X_LABEL>" in content_str
    assert "<DVC_METRIC_Y_LABEL>" in content_str

    def _assert_templates_equal(
        html_template, filled_template, split_template, title, x_label, y_label
    ):
        # besides split anchors, json and split json should be equal
        paths = [["data", "values"], ["encoding", "color"]]
        tmp1 = deepcopy(html_template)
        tmp2 = deepcopy(filled_template)
        tmp3 = json.loads(
            json.dumps(split_template)
            .replace('"<DVC_METRIC_PLOT_HEIGHT>"', "300")
            .replace('"<DVC_METRIC_PLOT_WIDTH>"', "300")
            .replace("<DVC_METRIC_TITLE>", title)
            .replace("<DVC_METRIC_X_LABEL>", x_label)
            .replace("<DVC_METRIC_Y_LABEL>", y_label)
            .replace(
                '"<DVC_METRIC_ZOOM_AND_PAN>"',
                json.dumps({"name": "grid", "select": "interval", "bind": "scales"}),
            )
        )
        for path in paths:
            dpath.set(tmp1, path, {})
            dpath.set(tmp2, path, {})
            dpath.set(tmp3, path, {})

        assert tmp1 == tmp2 == tmp3

    _assert_templates_equal(
        html_result,
        json_result[0]["content"],
        split_json_result[0]["content"],
        title,
        x_label,
        y_label,
    )


def verify_vega_props(plot_id, json_result, title, x, y, **kwargs):
    data = json_result[plot_id]
    assert len(data) == 1
    data = first(data)

    assert dpath.get(data, ["content", "title", "text"]) == title

    try:
        # TODO confusion_matrix_plot - need to find better way of asserting
        #      encoding as its place is not constant in vega
        plot_x = dpath.get(data, ["content", "spec", "encoding", "x", "field"])
        plot_y = dpath.get(data, ["content", "spec", "encoding", "y", "field"])
    except KeyError:
        # default plot
        plot_x = dpath.get(data, ["content", "layer", 0, "encoding", "x", "field"])
        plot_y = dpath.get(data, ["content", "layer", 0, "encoding", "y", "field"])

    assert plot_x == x
    assert plot_y == y


def _update_datapoints(datapoints: list, update: dict):
    result = []
    for dp in datapoints:
        tmp = dp.copy()
        tmp.update(update)
        result.append(tmp)
    return result


@pytest.mark.vscode
def test_no_plots(tmp_dir, scm, dvc, capsys):
    html_path, json_result, split_json_result = call(capsys)
    assert not os.path.exists(html_path)
    assert json_result == {}
    assert split_json_result == {}


@pytest.mark.vscode
def test_repo_with_plots(tmp_dir, scm, dvc, capsys, run_copy_metrics, repo_with_plots):
    repo_state = repo_with_plots()

    image_v1, linear_v1, confusion_v1, confusion_props = next(repo_state)

    html_path, json_result, split_json_result = call(capsys)
    html_result = extract_vega_specs(html_path, ["linear.json", "confusion.json"])

    assert "errors" not in json_result
    assert "errors" not in split_json_result

    json_data = json_result["data"]
    split_json_data = split_json_result["data"]

    assert json_data["linear.json"][0]["content"]["data"][
        "values"
    ] == _update_datapoints(
        linear_v1,
        {
            REVISION: "workspace",
        },
    )
    assert html_result["linear.json"]["data"]["values"] == _update_datapoints(
        linear_v1,
        {
            REVISION: "workspace",
        },
    )
    assert json_data["confusion.json"][0]["content"]["data"][
        "values"
    ] == _update_datapoints(
        confusion_v1,
        {
            REVISION: "workspace",
        },
    )
    assert html_result["confusion.json"]["data"]["values"] == _update_datapoints(
        confusion_v1,
        {
            REVISION: "workspace",
        },
    )
    verify_image(tmp_dir, "workspace", "image.png", image_v1, html_path, json_data)

    for plot, title, x_label, y_label in [
        ("linear.json", "linear", "x", "y"),
        ("confusion.json", "confusion matrix", "predicted", "actual"),
    ]:
        verify_vega(
            "workspace",
            html_result[plot],
            json_data[plot],
            split_json_data[plot],
            title,
            x_label,
            y_label,
        )

    verify_vega_props("confusion.json", json_data, **confusion_props)

    image_v2, linear_v2, confusion_v2, confusion_props = next(repo_state)

    html_path, json_result, split_json_result = call(capsys, subcommand="diff")
    html_result = extract_vega_specs(html_path, ["linear.json", "confusion.json"])

    assert "errors" not in json_result
    assert "errors" not in split_json_result

    json_data = json_result["data"]
    split_json_data = split_json_result["data"]

    verify_image(tmp_dir, "workspace", "image.png", image_v2, html_path, json_data)
    verify_image(tmp_dir, "HEAD", "image.png", image_v1, html_path, json_data)

    for plot, title, x_label, y_label in [
        ("linear.json", "linear", "x", "y"),
        ("confusion.json", "confusion matrix", "predicted", "actual"),
    ]:
        verify_vega(
            ["HEAD", "workspace"],
            html_result[plot],
            json_data[plot],
            split_json_data[plot],
            title,
            x_label,
            y_label,
        )
    verify_vega_props("confusion.json", json_data, **confusion_props)
    path = tmp_dir / "subdir"
    path.mkdir()
    with path.chdir():
        html_path, json_result, split_json_result = call(capsys, subcommand="diff")
        html_result = extract_vega_specs(
            html_path,
            ["../linear.json", "../confusion.json"],
        )

        assert "errors" not in json_result
        assert "errors" not in split_json_result

        json_data = json_result["data"]
        split_json_data = split_json_result["data"]
        assert json_data["../linear.json"][0]["content"]["data"][
            "values"
        ] == _update_datapoints(
            linear_v2,
            {
                REVISION: "workspace",
            },
        ) + _update_datapoints(
            linear_v1,
            {
                REVISION: "HEAD",
            },
        )
        assert html_result["../linear.json"]["data"]["values"] == _update_datapoints(
            linear_v2,
            {
                REVISION: "workspace",
            },
        ) + _update_datapoints(
            linear_v1,
            {
                REVISION: "HEAD",
            },
        )
        assert json_data["../confusion.json"][0]["content"]["data"][
            "values"
        ] == _update_datapoints(
            confusion_v2,
            {
                REVISION: "workspace",
            },
        ) + _update_datapoints(
            confusion_v1,
            {
                REVISION: "HEAD",
            },
        )
        assert html_result["../confusion.json"]["data"]["values"] == _update_datapoints(
            confusion_v2,
            {
                REVISION: "workspace",
            },
        ) + _update_datapoints(
            confusion_v1,
            {
                REVISION: "HEAD",
            },
        )

        for plot, title, x_label, y_label in [
            ("../linear.json", "linear", "x", "y"),
            ("../confusion.json", "confusion matrix", "predicted", "actual"),
        ]:
            verify_vega(
                ["HEAD", "workspace"],
                html_result[plot],
                json_data[plot],
                split_json_data[plot],
                title,
                x_label,
                y_label,
            )
        verify_image(path, "workspace", "../image.png", image_v2, html_path, json_data)
        verify_image(path, "HEAD", "../image.png", image_v1, html_path, json_data)


@pytest.mark.vscode
def test_repo_with_removed_plots(tmp_dir, capsys, repo_with_plots):
    from dvc.utils.fs import remove

    next(repo_with_plots())

    # even if there is no data, call should be successful
    remove(tmp_dir / ".dvc" / "cache")
    remove("linear.json")
    remove("confusion.json")
    remove("image.png")

    for s in ("show", "diff"):
        _, json_result, split_json_result = call(capsys, subcommand=s)
        errors = [
            {
                "name": p,
                "source": p,
                "rev": "workspace",
                "type": "FileNotFoundError",
                "msg": f"[Errno 2] No storage files available: '{p}'",
            }
            for p in [
                "linear.json",
                "confusion.json",
                "image.png",
            ]
        ]
        expected_result = {
            "errors": errors,
            "data": {
                "image.png": [],
                "confusion.json": [],
                "linear.json": [],
            },
        }
        assert json_result == expected_result
        assert split_json_result == expected_result


def test_config_output_dir(tmp_dir, dvc, capsys):
    subdir = tmp_dir / "subdir"
    ret = main(["config", "plots.out_dir", os.fspath(subdir)])
    assert ret == 0

    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    (tmp_dir / "metric.json").dump_json(metric, sort_keys=True)

    assert main(["plots", "show", "metric.json"]) == 0

    out, _ = capsys.readouterr()
    assert subdir.as_uri() in out
    assert subdir.is_dir()
    assert (subdir / "index.html").is_file()

    cli_arg_subdir = tmp_dir / "cli_option"
    assert main(["plots", "show", "-o", os.fspath(cli_arg_subdir), "metric.json"]) == 0

    out, _ = capsys.readouterr()
    assert cli_arg_subdir.as_uri() in out
    assert cli_arg_subdir.is_dir()
    assert (cli_arg_subdir / "index.html").is_file()


@pytest.mark.vscode
def test_repo_with_config_plots(tmp_dir, capsys, repo_with_config_plots):
    repo_state = repo_with_config_plots()
    plots = next(repo_state)

    html_path, _, split_json_result = call(capsys)

    assert os.path.exists(html_path)
    html_result = extract_vega_specs(
        html_path,
        [
            "subdirA/linear_subdir.json",
            "subdirB/linear_subdir.json",
            "linear_train_vs_test",
            "confusion_train_vs_test",
        ],
    )

    ble = _update_datapoints(
        plots["data"]["linear_train.json"],
        {
            REVISION: "workspace",
            FILENAME: "linear_train.json",
        },
    ) + _update_datapoints(
        plots["data"]["linear_test.json"],
        {
            REVISION: "workspace",
            FILENAME: "linear_test.json",
        },
    )

    assert html_result["linear_train_vs_test"]["data"]["values"] == ble
    assert (
        split_json_result["data"]["linear_train_vs_test"][0][ANCHOR_DEFINITIONS][
            "<DVC_METRIC_DATA>"
        ]
        == ble
    )
    assert (
        html_result["subdirA/linear_subdir.json"]["layer"][1]["encoding"]["x"]["field"]
        == "x"
    )
    assert (
        html_result["subdirA/linear_subdir.json"]["layer"][1]["encoding"]["y"]["field"]
        == "y"
    )
    assert (
        html_result["subdirB/linear_subdir.json"]["layer"][1]["encoding"]["x"]["field"]
        == "step"
    )
    assert (
        html_result["subdirB/linear_subdir.json"]["layer"][1]["encoding"]["y"]["field"]
        == "y"
    )


@pytest.mark.vscode
def test_repo_with_dvclive_plots(tmp_dir, capsys, repo_with_dvclive_plots):
    next(repo_with_dvclive_plots())

    for s in ("show", "diff"):
        _, json_result, split_json_result = call(capsys, subcommand=s)
        expected_result: dict[str, dict[str, list[str]]] = {
            "data": {
                "dvclive/plots/metrics/metric.tsv": [],
            },
        }
        assert json_result == expected_result
        assert split_json_result == expected_result


@pytest.mark.vscode
def test_nested_x_defn_collection(tmp_dir, dvc, scm, capsys):
    rel_pipeline_dir = "pipelines/data-increment"
    pipeline_rel_dvclive_metrics_dir = "dvclive/plots/metrics"
    pipeline_rel_other_logger_dir = "other/logger"

    dvc_rel_dvclive_metrics_dir = (
        f"{rel_pipeline_dir}/{pipeline_rel_dvclive_metrics_dir}"
    )
    dvc_rel_other_logger_dir = f"{rel_pipeline_dir}/{pipeline_rel_other_logger_dir}"

    pipeline_dir = tmp_dir / rel_pipeline_dir
    dvclive_metrics_dir = pipeline_dir / pipeline_rel_dvclive_metrics_dir
    dvclive_metrics_dir.mkdir(parents=True)
    other_logger_dir = pipeline_dir / pipeline_rel_other_logger_dir
    other_logger_dir.mkdir(parents=True)

    (pipeline_dir / "dvc.yaml").dump(
        {
            "plots": [
                {
                    "Error vs max_leaf_nodes": {
                        "template": "simple",
                        "x": {
                            f"{pipeline_rel_dvclive_metrics_dir}"
                            "/Max_Leaf_Nodes.tsv": "Max_Leaf_Nodes"
                        },
                        "y": {f"{pipeline_rel_dvclive_metrics_dir}/Error.tsv": "Error"},
                    }
                },
                {
                    f"{pipeline_rel_other_logger_dir}/multiple_metrics.json": {
                        "x": "x",
                        "y": ["y1", "y2"],
                    },
                },
                {
                    f"{pipeline_rel_dvclive_metrics_dir}/Error.tsv": {"y": ["Error"]},
                },
                {
                    "max leaf nodes": {
                        "y": {
                            f"{pipeline_rel_dvclive_metrics_dir}"
                            "/Max_Leaf_Nodes.tsv": "Max_Leaf_Nodes"
                        }
                    },
                },
            ]
        },
    )
    dvclive_metrics_dir.gen(
        {
            "Error.tsv": "step\tError\n" "0\t0.11\n" "1\t0.22\n" "2\t0.44\n",
            "Max_Leaf_Nodes.tsv": "step\tMax_Leaf_Nodes\n"
            "0\t5\n"
            "1\t50\n"
            "2\t500\n",
        }
    )
    (other_logger_dir / "multiple_metrics.json").dump(
        [
            {"x": 0, "y1": 0.1, "y2": 10},
            {"x": 1, "y1": 0.2, "y2": 22},
        ]
    )

    scm.commit("add dvc.yaml and metrics")

    _, _, split_json_result = call(capsys, subcommand="diff")
    assert len(split_json_result.keys()) == 1
    assert len(split_json_result["data"].keys()) == 4

    separate_x_file = split_json_result["data"]["Error vs max_leaf_nodes"][0]

    assert separate_x_file["anchor_definitions"]["<DVC_METRIC_DATA>"] == [
        {"Error": "0.11", "Max_Leaf_Nodes": "5", "step": "0", "rev": "workspace"},
        {"Error": "0.22", "Max_Leaf_Nodes": "50", "step": "1", "rev": "workspace"},
        {"Error": "0.44", "Max_Leaf_Nodes": "500", "step": "2", "rev": "workspace"},
    ]

    same_x_file = split_json_result["data"][
        f"{dvc_rel_other_logger_dir}/multiple_metrics.json"
    ][0]
    assert same_x_file["anchor_definitions"]["<DVC_METRIC_DATA>"] == [
        {
            "x": 0,
            "y1": 0.1,
            "y2": 10,
            "dvc_inferred_y_value": 0.1,
            "field": "y1",
            "rev": "workspace",
        },
        {
            "x": 1,
            "y1": 0.2,
            "y2": 22,
            "dvc_inferred_y_value": 0.2,
            "field": "y1",
            "rev": "workspace",
        },
        {
            "x": 0,
            "y1": 0.1,
            "y2": 10,
            "dvc_inferred_y_value": 10,
            "field": "y2",
            "rev": "workspace",
        },
        {
            "x": 1,
            "y1": 0.2,
            "y2": 22,
            "dvc_inferred_y_value": 22,
            "field": "y2",
            "rev": "workspace",
        },
    ]

    inferred_x_from_str = split_json_result["data"][
        f"{dvc_rel_dvclive_metrics_dir}/Error.tsv"
    ][0]
    assert inferred_x_from_str["anchor_definitions"]["<DVC_METRIC_DATA>"] == [
        {"step": 0, "Error": "0.11", "rev": "workspace"},
        {"step": 1, "Error": "0.22", "rev": "workspace"},
        {"step": 2, "Error": "0.44", "rev": "workspace"},
    ]

    inferred_x_from_dict = split_json_result["data"]["max leaf nodes"][0]
    assert inferred_x_from_dict["anchor_definitions"]["<DVC_METRIC_DATA>"] == [
        {"step": 0, "Max_Leaf_Nodes": "5", "rev": "workspace"},
        {"step": 1, "Max_Leaf_Nodes": "50", "rev": "workspace"},
        {"step": 2, "Max_Leaf_Nodes": "500", "rev": "workspace"},
    ]


def test_plots_empty_directory(tmp_dir, dvc, scm, capsys):
    (tmp_dir / "empty").mkdir()
    (tmp_dir / "dvc.yaml").dump({"plots": [{"empty": {}}]})

    scm.add(["dvc.yaml"])
    scm.commit("commit dvc files")

    html_path, _, split_json_result = call(capsys)
    assert split_json_result == {}
    assert html_path == ""
