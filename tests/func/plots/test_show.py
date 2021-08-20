import os

import pytest
from funcy import get_in

from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import OverlappingOutputPathsError
from dvc.main import main
from dvc.path_info import PathInfo
from dvc.render.vega import PlotMetricTypeError
from dvc.repo import Repo
from dvc.utils import onerror_collect
from dvc.utils.fs import remove
from dvc.utils.serialize import (
    EncodingError,
    YAMLFileCorruptedError,
    dump_yaml,
    modify_yaml,
)
from tests.func.plots.utils import _write_json


def test_plot_cache_missing(tmp_dir, scm, dvc, caplog, run_copy_metrics):
    metric1 = [{"y": 2}, {"y": 3}]
    _write_json(tmp_dir, metric1, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        commit="there is metric",
    )
    scm.tag("v1")

    # Make a different plot and then remove its datafile
    metric2 = [{"y": 3}, {"y": 4}]
    _write_json(tmp_dir, metric2, "metric_t.json")
    stage = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        commit="there is an another metric",
    )
    scm.tag("v2")
    remove(stage.outs[0].fspath)
    remove(stage.outs[0].cache_path)

    plots_data = dvc.plots.show(revs=["v1", "v2"], targets=["metric.json"])
    assert plots_data["v1"]["data"]["metric.json"]["data"] == metric1
    assert isinstance(
        plots_data["v2"]["data"]["metric.json"]["error"], FileNotFoundError
    )


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


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_plot(tmp_dir, scm, use_dvc):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(tmp_dir, metric, "metric.json")

    if use_dvc:
        dvc = Repo.init()
    else:
        dvc = Repo(uninitialized=True)

    plots = dvc.plots.show(targets=["metric.json"])

    assert plots["workspace"]["data"]["metric.json"]["data"] == metric


def test_show_non_plot_and_plot_with_params(
    tmp_dir, scm, dvc, run_copy_metrics
):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(tmp_dir, metric, "metric.json")
    run_copy_metrics(
        "metric.json", "metric2.json", plots_no_cache=["metric2.json"]
    )
    props = {"title": "TITLE"}
    dvc.plots.modify("metric2.json", props=props)

    result = dvc.plots.show(targets=["metric.json", "metric2.json"])
    assert "metric.json" in result["workspace"]["data"]
    assert "metric2.json" in result["workspace"]["data"]
    assert result["workspace"]["data"]["metric2.json"]["props"] == props


def test_show_from_subdir(tmp_dir, dvc, capsys):
    subdir = tmp_dir / "subdir"

    subdir.mkdir()
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    _write_json(subdir, metric, "metric.json")

    with subdir.chdir():
        assert main(["plots", "show", "metric.json"]) == 0

    out, _ = capsys.readouterr()
    assert subdir.as_uri() in out
    assert (subdir / "dvc_plots").is_dir()
    assert (subdir / "dvc_plots" / "index.html").is_file()


def test_plots_show_non_existing(tmp_dir, dvc, caplog):
    result = dvc.plots.show(targets=["plot.json"])
    assert isinstance(
        result["workspace"]["data"]["plot.json"]["error"], FileNotFoundError
    )

    assert "'plot.json' was not found in current workspace." in caplog.text


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


def test_dir_plots(tmp_dir, dvc, run_copy_metrics):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]

    fname = "file.json"
    _write_json(tmp_dir, metric, fname)

    p1 = os.path.join("subdir", "p1.json")
    p2 = os.path.join("subdir", "p2.json")
    tmp_dir.dvc.run(
        cmd=(
            f"mkdir subdir && python copy.py {fname} {p1} && "
            f"python copy.py {fname} {p2}"
        ),
        deps=[fname],
        single_stage=False,
        plots=["subdir"],
        name="copy_double",
    )
    props = {"title": "TITLE"}
    dvc.plots.modify("subdir", {"title": "TITLE"})

    result = dvc.plots.show()
    assert set(result["workspace"]["data"]) == {p1, p2}
    assert result["workspace"]["data"][p1]["props"] == props
    assert result["workspace"]["data"][p2]["props"] == props


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

    dvc.add(["image.jpg"])
    run_copy_metrics(
        "image.jpg",
        "plot.jpg",
        commit="run training",
        plots=["plot.jpg"],
        name="s2",
        single_stage=False,
    )

    scm.add(["dvc.yaml", "dvc.lock"])
    scm.commit("initial")

    scm.tag("v1")

    with open("plot.jpg", "wb") as fd:
        fd.write(b"content2")

    result = dvc.plots.show(revs=["v1", "workspace"])
    assert result["v1"]["data"]["plot.jpg"]["data"] == b"content"
    assert result["workspace"]["data"]["plot.jpg"]["data"] == b"content2"
