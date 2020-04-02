import json
import os
from copy import copy

from bs4 import BeautifulSoup
from funcy import first

from dvc.plot import Template


def _add_revision(data, rev="current workspace"):
    new_data = copy(data)
    for e in new_data:
        e["revision"] = rev

    return new_data


def test_plot_linear(tmp_dir, dvc):
    metric = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    tmp_dir.dvc_gen({"metric.json": json.dumps(metric)})
    dvc.metrics.add("metric.json")

    dvc.plot(["metric.json"], "result.html")

    page = tmp_dir / "result.html"

    assert page.exists()
    page_content = BeautifulSoup(page.read_text())

    with_revision = _add_revision(metric)
    expected_script_content = json.dumps(
        Template.fill(
            os.path.join(dvc.plot_templates.templates_dir, "default.json"),
            with_revision,
            "metric.json",
        ),
        indent=4,
        separators=(",", ": "),
    )

    assert expected_script_content in first(page_content.body.script.contents)


def test_plot_confusion(tmp_dir, dvc):
    confusion_matrix = [{"x": "B", "y": "A"}, {"x": "A", "y": "A"}]
    tmp_dir.dvc_gen({"metric.json": json.dumps(confusion_matrix)})
    dvc.metrics.add("metric.json")

    dvc.plot(
        ["metric.json"],
        "result.html",
        template=os.path.join(
            dvc.plot_templates.templates_dir, "default_confusion.json"
        ),
    )

    page = tmp_dir / "result.html"

    assert page.exists()
    page_content = BeautifulSoup(page.read_text())

    with_revision = _add_revision(confusion_matrix)
    expected_script_content = json.dumps(
        Template.fill(
            os.path.join(".dvc", "plot", "default_confusion.json"),
            with_revision,
            "metric.json",
        ),
        indent=4,
        separators=(",", ": "),
    )

    assert expected_script_content in first(page_content.body.script.contents)
