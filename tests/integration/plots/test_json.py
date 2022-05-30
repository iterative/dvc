import json
import os
from copy import deepcopy
from typing import Dict, List
from urllib.parse import urlparse
from urllib.request import url2pathname

import dpath.util
import pytest
from funcy import first

from dvc.cli import main

JSON_OUT = "vis_data"


def call(capsys, subcommand="show"):
    capsys.readouterr()
    assert (
        main(["plots", subcommand, "--json", "-o", JSON_OUT, "--split"]) == 0
    )
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


def filter_fields(datapoints: List[Dict], fields: List[str]):

    tmp = deepcopy(datapoints)
    for datapoint in tmp:
        keys = set(datapoint.keys())
        for key in keys:
            if key not in fields:
                datapoint.pop(key)
    return tmp


def verify_image(tmp_dir, version, filename, content, html_path, json_result):
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
    assert image_data["url"] == str(
        tmp_dir / JSON_OUT / f"{version}_{filename}"
    )
    assert (
        tmp_dir / JSON_OUT / f"{version}_{filename}"
    ).read_bytes() == content

    assert os.path.join("static", f"{version}_{filename}") in html_content

    # there should be no absolute paths in produced HTML
    # TODO uncomment once dvc-render is adjusted
    # assert str(tmp_dir) not in html_content
    assert (
        tmp_dir / "dvc_plots" / "static" / f"{version}_{filename}"
    ).read_bytes() == content


def _remove_blanks(text: str):
    return text.replace("\t", "").replace("\n", "").replace(" ", "")


def verify_vega(
    versions,
    filename,
    fields,
    expected_data,
    html_path,
    json_result,
    split_json_result,
):

    assert os.path.exists(html_path)
    with open(html_path, encoding="utf-8") as fd:
        html_content = fd.read()
    assert _remove_blanks(
        json.dumps(dpath.util.get(json_result, [filename, 0, "content"]))
    ) in _remove_blanks(html_content)

    if isinstance(versions, str):
        versions = [versions]

    for j in [json_result, split_json_result]:
        assert len(j[filename]) == 1
        assert dpath.util.get(j, [filename, 0, "type"]) == "vega"
        assert dpath.util.get(j, [filename, 0, "revisions"]) == versions

    assert dpath.util.get(
        json_result, [filename, 0, "datapoints"]
    ) == dpath.util.get(split_json_result, [filename, 0, "datapoints"])
    assert set(versions) == set(
        dpath.util.get(json_result, [filename, 0, "datapoints"]).keys()
    )

    assert (
        filter_fields(
            dpath.util.get(
                json_result, [filename, 0, "content", "data", "values"]
            ),
            fields,
        )
        == expected_data
    )

    assert (
        dpath.util.get(
            split_json_result, [filename, 0, "content", "data", "values"]
        )
        == "<DVC_METRIC_DATA>"
    )

    # if data is not considered, json and split json should be identical
    json_content = deepcopy(
        dpath.util.get(json_result, [filename, 0, "content"])
    )
    split_json_content = deepcopy(
        dpath.util.get(split_json_result, [filename, 0, "content"])
    )
    dpath.util.set(json_content, ["data"], {})
    dpath.util.set(split_json_content, ["data"], {})
    assert json_content == split_json_content


def verify_vega_props(filename, json_result, title, x, y, **kwargs):
    data = json_result[filename]
    assert len(data) == 1
    data = first(data)

    assert dpath.util.get(data, ["content", "title"]) == title

    assert (
        dpath.util.get(data, ["content", "spec", "encoding", "x", "field"])
        == x
    )
    assert (
        dpath.util.get(data, ["content", "spec", "encoding", "y", "field"])
        == y
    )


@pytest.mark.vscode
def test_no_plots(tmp_dir, scm, dvc, capsys):
    html_path, json_result, split_json_result = call(capsys)
    assert os.path.exists(html_path)
    assert json_result == {}
    assert split_json_result == {}


@pytest.mark.vscode
def test_repo_with_plots(
    tmp_dir, scm, dvc, capsys, run_copy_metrics, repo_with_plots
):
    repo_state = repo_with_plots()

    image_v1, linear_v1, confusion_v1, confusion_props = next(repo_state)

    html_path, json_result, split_json_result = call(capsys)

    verify_image(
        tmp_dir, "workspace", "image.png", image_v1, html_path, json_result
    )

    verify_vega(
        "workspace",
        "linear.json",
        ["x", "y"],
        linear_v1,
        html_path,
        json_result,
        split_json_result,
    )

    verify_vega(
        "workspace",
        "confusion.json",
        ["actual", "predicted"],
        confusion_v1,
        html_path,
        json_result,
        split_json_result,
    )
    verify_vega_props("confusion.json", json_result, **confusion_props)

    image_v2, linear_v2, confusion_v2, confusion_props = next(repo_state)

    html_path, json_result, split_json_result = call(capsys, subcommand="diff")

    verify_image(
        tmp_dir, "workspace", "image.png", image_v2, html_path, json_result
    )
    verify_image(
        tmp_dir, "HEAD", "image.png", image_v1, html_path, json_result
    )

    verify_vega(
        ["HEAD", "workspace"],
        "linear.json",
        ["x", "y"],
        linear_v2 + linear_v1,
        html_path,
        json_result,
        split_json_result,
    )

    verify_vega(
        ["HEAD", "workspace"],
        "confusion.json",
        ["actual", "predicted"],
        confusion_v2 + confusion_v1,
        html_path,
        json_result,
        split_json_result,
    )
    verify_vega_props("confusion.json", json_result, **confusion_props)


@pytest.mark.vscode
def test_repo_with_removed_plots(tmp_dir, capsys, repo_with_plots):
    from dvc.utils.fs import remove

    next(repo_with_plots())

    # even if there is no data, call should be successful
    remove(tmp_dir / ".dvc" / "cache")
    remove("linear.json")
    remove("confusion.json")
    remove("image.png")

    for s in {"show", "diff"}:
        _, json_result, split_json_result = call(capsys, subcommand=s)
        for p in {"linear.json", "confusion.json", "image.png"}:
            assert json_result[p] == []
            assert split_json_result[p] == []
