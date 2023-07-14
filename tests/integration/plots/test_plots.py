import json
import os
from copy import deepcopy
from typing import Dict, List
from urllib.parse import urlparse
from urllib.request import url2pathname

import dpath
import pytest
from bs4 import BeautifulSoup
from funcy import first

from dvc.cli import main
from dvc.render import REVISION_FIELD, VERSION_FIELD

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


def drop_fields(datapoints: List[Dict], fields: List[str]):
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
    versions,
    html_result,
    json_result,
    split_json_result,
):
    if isinstance(versions, str):
        versions = [versions]

    for j in [json_result, split_json_result]:
        assert len(j) == 1
        assert j[0]["type"] == "vega"
        assert set(j[0]["revisions"]) == set(versions)

    assert json_result[0]["datapoints"] == split_json_result[0]["datapoints"]
    assert set(versions) == set(json_result[0]["datapoints"].keys())

    assert json_result[0]["content"]["data"]["values"]
    assert html_result["data"]["values"]
    assert split_json_result[0]["content"]["data"]["values"] == "<DVC_METRIC_DATA>"

    def _assert_templates_equal(html_template, filled_template, split_template):
        # besides data, json and split json should be equal
        path = ["data", "values"]
        tmp1 = deepcopy(html_template)
        tmp2 = deepcopy(filled_template)
        tmp3 = deepcopy(split_template)
        dpath.set(tmp1, path, {})
        dpath.set(tmp2, path, {})
        dpath.set(tmp3, path, {})

        assert tmp1 == tmp2 == tmp3

    _assert_templates_equal(
        html_result, json_result[0]["content"], split_json_result[0]["content"]
    )


def verify_vega_props(plot_id, json_result, title, x, y, **kwargs):
    data = json_result[plot_id]
    assert len(data) == 1
    data = first(data)

    assert dpath.get(data, ["content", "title"]) == title

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


def _update_datapoints(datapoints: List, update: Dict):
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
            VERSION_FIELD: {
                "revision": "workspace",
                "filename": "linear.json",
                "field": "y",
            },
        },
    )
    assert html_result["linear.json"]["data"]["values"] == _update_datapoints(
        linear_v1,
        {
            REVISION_FIELD: "workspace",
        },
    )
    assert json_data["confusion.json"][0]["content"]["data"][
        "values"
    ] == _update_datapoints(
        confusion_v1,
        {
            VERSION_FIELD: {
                "revision": "workspace",
                "filename": "confusion.json",
                "field": "actual",
            },
        },
    )
    assert html_result["confusion.json"]["data"]["values"] == _update_datapoints(
        confusion_v1,
        {
            REVISION_FIELD: "workspace",
        },
    )
    verify_image(tmp_dir, "workspace", "image.png", image_v1, html_path, json_data)

    for plot in ["linear.json", "confusion.json"]:
        verify_vega(
            "workspace",
            html_result[plot],
            json_data[plot],
            split_json_data[plot],
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

    for plot in ["linear.json", "confusion.json"]:
        verify_vega(
            ["HEAD", "workspace"],
            html_result[plot],
            json_data[plot],
            split_json_data[plot],
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
                VERSION_FIELD: {
                    "revision": "workspace",
                    "filename": "../linear.json",
                    "field": "y",
                },
            },
        ) + _update_datapoints(
            linear_v1,
            {
                VERSION_FIELD: {
                    "revision": "HEAD",
                    "filename": "../linear.json",
                    "field": "y",
                },
            },
        )
        assert html_result["../linear.json"]["data"]["values"] == _update_datapoints(
            linear_v2,
            {
                REVISION_FIELD: "workspace",
            },
        ) + _update_datapoints(
            linear_v1,
            {
                REVISION_FIELD: "HEAD",
            },
        )
        assert json_data["../confusion.json"][0]["content"]["data"][
            "values"
        ] == _update_datapoints(
            confusion_v2,
            {
                VERSION_FIELD: {
                    "revision": "workspace",
                    "filename": "../confusion.json",
                    "field": "actual",
                },
            },
        ) + _update_datapoints(
            confusion_v1,
            {
                VERSION_FIELD: {
                    "revision": "HEAD",
                    "filename": "../confusion.json",
                    "field": "actual",
                },
            },
        )
        assert html_result["../confusion.json"]["data"]["values"] == _update_datapoints(
            confusion_v2,
            {
                REVISION_FIELD: "workspace",
            },
        ) + _update_datapoints(
            confusion_v1,
            {
                REVISION_FIELD: "HEAD",
            },
        )

        for plot in [
            "../linear.json",
            "../confusion.json",
        ]:
            verify_vega(
                ["HEAD", "workspace"],
                html_result[plot],
                json_data[plot],
                split_json_data[plot],
            )
        verify_image(
            path,
            "workspace",
            "../image.png",
            image_v2,
            html_path,
            json_data,
        )
        verify_image(
            path,
            "HEAD",
            "../image.png",
            image_v1,
            html_path,
            json_data,
        )


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

    html_path, _, __ = call(capsys)

    assert os.path.exists(html_path)
    html_result = extract_vega_specs(
        html_path,
        [
            "dvc.yaml::linear_train_vs_test",
            "dvc.yaml::confusion_train_vs_test",
        ],
    )
    ble = _update_datapoints(
        plots["data"]["linear_train.json"],
        {
            REVISION_FIELD: "linear_train.json",
        },
    ) + _update_datapoints(
        plots["data"]["linear_test.json"],
        {
            REVISION_FIELD: "linear_test.json",
        },
    )

    assert html_result["dvc.yaml::linear_train_vs_test"]["data"]["values"] == ble
    # TODO check json results once vscode is able to handle flexible plots
