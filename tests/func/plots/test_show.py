import os

import pytest
from funcy import get_in

from dvc.cli import main
from dvc.dvcfile import PIPELINE_FILE
from dvc.exceptions import OverlappingOutputPathsError
from dvc.repo import Repo
from dvc.repo.plots import PlotMetricTypeError
from dvc.utils import onerror_collect
from dvc.utils.fs import remove
from dvc.utils.serialize import EncodingError, YAMLFileCorruptedError


def test_plot_cache_missing(tmp_dir, scm, dvc, caplog, run_copy_metrics):
    metric1 = [{"y": 2}, {"y": 3}]
    (tmp_dir / "metric_t.json").dump_json(metric1, sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        commit="there is metric",
    )
    scm.tag("v1")

    # Make a different plot and then remove its datafile
    metric2 = [{"y": 3}, {"y": 4}]
    (tmp_dir / "metric_t.json").dump_json(metric2, sort_keys=True)
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
        dvc.plots.show(targets=["metric.txt"], onerror=onerror_collect)[
            "workspace"
        ]["data"]["metric.txt"]["error"],
        PlotMetricTypeError,
    )


@pytest.mark.parametrize("use_dvc", [True, False])
def test_show_non_plot(tmp_dir, scm, use_dvc):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    (tmp_dir / "metric.json").dump_json(metric, sort_keys=True)

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
    (tmp_dir / "metric.json").dump_json(metric, sort_keys=True)
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
    (subdir / "metric.json").dump_json(metric, sort_keys=True)

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
    data_dir = tmp_dir / "data"
    data_dir.mkdir()

    (data_dir / "m1_temp.yaml").dump({"a": {"b": {"c": 2, "d": 1}}})
    run_copy_metrics(
        str(data_dir / "m1_temp.yaml"),
        str(data_dir / "m1.yaml"),
        single_stage=False,
        commit="add m1",
        name="cp-m1",
        plots=[str(data_dir / "m1.yaml")],
    )
    with (tmp_dir / "dvc.yaml").modify() as d:
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
        dvc.plots.show(onerror=onerror_collect)["workspace"]["error"],
        OverlappingOutputPathsError,
    )


def test_dir_plots(tmp_dir, dvc, run_copy_metrics):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]

    fname = "file.json"
    (tmp_dir / fname).dump_json(metric, sort_keys=True)

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
    dvc.plots.modify("subdir", props)

    result = dvc.plots.show()
    assert set(result["workspace"]["data"]) == {p1, p2}
    assert result["workspace"]["data"][p1]["props"] == props
    assert result["workspace"]["data"][p2]["props"] == props


def test_ignore_parsing_error(tmp_dir, dvc, run_copy_metrics):
    with open("file", "wb", encoding=None) as fobj:
        fobj.write(b"\xc1")

    run_copy_metrics("file", "plot_file.json", plots=["plot_file.json"])
    result = dvc.plots.show(onerror=onerror_collect)

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
    (tmp_dir / "metric_t.yaml").dump(metric)
    run_copy_metrics(
        "metric_t.yaml",
        "plot.yaml",
        plots=["plot.yaml"],
        single_stage=False,
        name="train",
    )
    scm.tag("v1")

    with open(file, "a", encoding="utf-8") as fd:
        fd.write("\nMALFORMED!")

    result = dvc.plots.show(onerror=onerror_collect)
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


def test_collect_non_existing_dir(tmp_dir, dvc, run_copy_metrics):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    subdir_metric = [{"y": 101, "x": 3}, {"y": 202, "x": 4}]

    pname = "source.json"
    (tmp_dir / pname).dump_json(metric, sort_keys=True)

    sname = "subdir_source.json"
    (tmp_dir / sname).dump_json(subdir_metric, sort_keys=True)

    p1 = os.path.join("subdir", "p1.json")
    p2 = os.path.join("subdir", "p2.json")
    subdir_stage = tmp_dir.dvc.run(
        cmd=(
            f"mkdir subdir && python copy.py {sname} {p1} && "
            f"python copy.py {sname} {p2}"
        ),
        deps=[sname],
        single_stage=False,
        plots=["subdir"],
        name="copy_double",
    )

    run_copy_metrics(
        pname,
        "plot.json",
        plots=["plot.json"],
        commit="there is metric",
    )

    remove(subdir_stage.outs[0].cache_path)
    remove(subdir_stage.outs[0].fs_path)

    result = dvc.plots.show()
    assert "error" in result["workspace"]["data"]["subdir"]
    # make sure others gets loaded
    assert result["workspace"]["data"]["plot.json"]["data"] == metric
