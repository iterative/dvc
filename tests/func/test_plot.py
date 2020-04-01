import json
from copy import copy

from bs4 import BeautifulSoup
from funcy import first

from dvc.plot import DefaultTemplate


def _add_revision(data, rev="current workspace"):
    new_data = copy(data)
    for e in new_data:
        e["revision"] = rev

    return new_data


def test_plot_vega_compliant_json(tmp_dir, dvc):
    metric = [{"x": 1, "y": 2}, {"x": 2, "y": 3}]
    tmp_dir.dvc_gen({"metric.json": json.dumps(metric)})
    dvc.metrics.add("metric.json")

    dvc.plot(["metric.json"], "result.html")

    page = tmp_dir / "result.html"

    assert page.exists()
    page_content = BeautifulSoup(page.read_text())

    with_revision = _add_revision(metric)
    expected_script_content = json.dumps(
        DefaultTemplate(dvc.dvc_dir).fill(with_revision, "metric.json"),
        indent=4,
        separators=(",", ": "),
    )

    assert expected_script_content in first(page_content.body.script.contents)
