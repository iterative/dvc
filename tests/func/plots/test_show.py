import json
import os

import pytest

from dvc.cli import main
from dvc.dvcfile import PROJECT_FILE
from dvc.exceptions import OverlappingOutputPathsError
from dvc.repo import Repo
from dvc.repo.plots import PlotMetricTypeError, onerror_collect
from dvc.utils.fs import remove
from dvc.utils.serialize import EncodingError, YAMLFileCorruptedError, modify_yaml
from tests.utils.plots import get_plot


def test_show_targets(tmp_dir, dvc):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    (tmp_dir / "metric.json").dump_json(metric, sort_keys=True)

    plots = dvc.plots.show(targets=["metric.json"])
    assert get_plot(plots, "workspace", file="metric.json") == metric

    plots = dvc.plots.show(targets=(tmp_dir / "metric.json").fs_path)
    assert get_plot(plots, "workspace", file="metric.json") == metric


def test_plot_cache_missing(tmp_dir, scm, dvc, caplog, run_copy_metrics):
    metric1 = [{"y": 2}, {"y": 3}]
    (tmp_dir / "metric_t.json").dump_json(metric1, sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        name="copy-metric",
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
        name="copy-metric",
        commit="there is an another metric",
    )
    scm.tag("v2")
    remove(stage.outs[0].fspath)
    remove(stage.outs[0].cache_path)

    plots_data = dvc.plots.show(revs=["v1", "v2"], targets=["metric.json"])

    assert get_plot(plots_data, "v1", file="metric.json") == metric1
    assert isinstance(
        get_plot(plots_data, "v2", file="metric.json", endkey="error"),
        FileNotFoundError,
    )


def test_plot_wrong_metric_type(tmp_dir, scm, dvc, run_copy_metrics):
    tmp_dir.gen("metric_t.txt", "some text")
    run_copy_metrics(
        "metric_t.txt",
        "metric.txt",
        plots_no_cache=["metric.txt"],
        name="copy-metric",
        commit="add text metric",
    )

    result = dvc.plots.show(targets=["metric.txt"], onerror=onerror_collect)
    assert isinstance(
        get_plot(result, "workspace", file="metric.txt", endkey="error"),
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

    assert get_plot(plots, "workspace", file="metric.json") == metric


def test_show_non_plot_and_plot_with_params(tmp_dir, scm, dvc, run_copy_metrics):
    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]
    (tmp_dir / "metric.json").dump_json(metric, sort_keys=True)
    run_copy_metrics(
        "metric.json",
        "metric2.json",
        plots_no_cache=["metric2.json"],
        name="train",
    )
    props = {"title": "TITLE"}
    dvc.plots.modify("metric2.json", props=props)

    result = dvc.plots.show(targets=["metric.json", "metric2.json"])

    assert get_plot(result, "workspace", file="metric.json") == metric
    assert get_plot(result, "workspace", file="metric2.json") == metric
    assert get_plot(result, "workspace", file="metric2.json", endkey="props") == props


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


def test_plots_show_non_existing(tmp_dir, dvc, capsys):
    result = dvc.plots.show(targets=["plot.json"])
    assert isinstance(
        get_plot(result, "workspace", file="plot.json", endkey="error"),
        FileNotFoundError,
    )

    cap = capsys.readouterr()
    assert (
        "DVC failed to load some plots for following revisions: 'workspace'" in cap.err
    )


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
        d["stages"]["corrupted-stage"] = {"cmd": "mkdir data", "outs": ["data"]}

    # running by clearing and not clearing stuffs
    # so as it works even for optimized cases
    if clear_before_run:
        remove(data_dir)
        remove(dvc.cache.local.path)

    dvc._reset()

    result = dvc.plots.show(onerror=onerror_collect)
    assert isinstance(
        get_plot(result, "workspace", endkey="error"),
        OverlappingOutputPathsError,
    )


def test_plots_show_nested_x_dict(tmp_dir, dvc, scm):
    rel_pipeline_dir = "pipelines/data-increment"

    pipeline_rel_dvclive_metrics_dir = "dvclive/plots/metrics"
    dvc_rel_dvclive_metrics_dir = (
        f"{rel_pipeline_dir}/{pipeline_rel_dvclive_metrics_dir}"
    )

    pipeline_dir = tmp_dir / rel_pipeline_dir
    dvclive_metrics_dir = pipeline_dir / pipeline_rel_dvclive_metrics_dir
    dvclive_metrics_dir.mkdir(parents=True)

    def _get_plot_defn(rel_dir: str) -> dict:
        return {
            "template": "simple",
            "x": {f"{rel_dir}/Max_Leaf_Nodes.tsv": "Max_Leaf_Nodes"},
            "y": {f"{rel_dir}/Error.tsv": "Error"},
        }

    (pipeline_dir / "dvc.yaml").dump(
        {
            "plots": [
                {
                    "Error vs max_leaf_nodes": _get_plot_defn(
                        pipeline_rel_dvclive_metrics_dir
                    )
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

    scm.commit("add dvc.yaml and dvclive metrics")

    result = dvc.plots.show()
    assert result == {
        "workspace": {
            "definitions": {
                "data": {
                    f"{rel_pipeline_dir}/dvc.yaml": {
                        "data": {
                            "Error vs max_leaf_nodes": _get_plot_defn(
                                dvc_rel_dvclive_metrics_dir
                            )
                        },
                    }
                }
            },
            "sources": {
                "data": {
                    f"{dvc_rel_dvclive_metrics_dir}/Error.tsv": {
                        "data": [
                            {"Error": "0.11", "step": "0"},
                            {"Error": "0.22", "step": "1"},
                            {"Error": "0.44", "step": "2"},
                        ],
                        "props": {},
                    },
                    f"{dvc_rel_dvclive_metrics_dir}/Max_Leaf_Nodes.tsv": {
                        "data": [
                            {"Max_Leaf_Nodes": "5", "step": "0"},
                            {"Max_Leaf_Nodes": "50", "step": "1"},
                            {"Max_Leaf_Nodes": "500", "step": "2"},
                        ],
                        "props": {},
                    },
                }
            },
        }
    }


def test_dir_plots(tmp_dir, dvc, run_copy_metrics):
    subdir = tmp_dir / "subdir"
    subdir.mkdir()

    metric = [{"first_val": 100, "val": 2}, {"first_val": 200, "val": 3}]

    fname = "file.json"
    (tmp_dir / fname).dump_json(metric, sort_keys=True)

    p1 = "subdir/p1.json"
    p2 = "subdir/p2.json"
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

    assert set(get_plot(result, "workspace")) == {p1, p2}
    assert get_plot(result, "workspace", typ="definitions", file="") == {
        p1: props,
        p2: props,
    }


def test_ignore_parsing_error(tmp_dir, dvc, run_copy_metrics):
    with open("file", "wb", encoding=None) as fobj:
        fobj.write(b"\xc1")

    run_copy_metrics(
        "file", "plot_file.json", plots=["plot_file.json"], name="copy-metric"
    )
    result = dvc.plots.show(onerror=onerror_collect)

    assert isinstance(
        get_plot(result, "workspace", file="plot_file.json", endkey="error"),
        EncodingError,
    )


@pytest.mark.parametrize(
    "file,path_kwargs",
    (
        (PROJECT_FILE, {"revision": "workspace", "endkey": "error"}),
        (
            "plot.yaml",
            {"revision": "workspace", "file": "plot.yaml", "endkey": "error"},
        ),
    ),
)
def test_log_errors(tmp_dir, scm, dvc, run_copy_metrics, file, path_kwargs, capsys):
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

    assert isinstance(get_plot(result, **path_kwargs), YAMLFileCorruptedError)
    assert (
        "DVC failed to load some plots for following revisions: 'workspace'." in error
    )


@pytest.mark.parametrize("ext", ["jpg", "svg"])
def test_plots_binary(tmp_dir, scm, dvc, run_copy_metrics, custom_template, ext):
    file1 = f"image.{ext}"
    file2 = f"plot.{ext}"
    with open(file1, "wb") as fd:
        fd.write(b"content")

    dvc.add([file1])
    run_copy_metrics(
        file1,
        file2,
        commit="run training",
        plots=[file2],
        name="s2",
        single_stage=False,
    )

    scm.add(["dvc.yaml", "dvc.lock"])
    scm.commit("initial")

    scm.tag("v1")

    with open(file2, "wb") as fd:
        fd.write(b"content2")

    result = dvc.plots.show(revs=["v1", "workspace"])
    assert get_plot(result, "v1", file=file2) == b"content"
    assert get_plot(result, "workspace", file=file2) == b"content2"


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
        plots=["subdir"],
        name="copy_double",
    )

    run_copy_metrics(
        pname,
        "plot.json",
        plots=["plot.json"],
        name="copy-metric",
        commit="there is metric",
    )

    remove(subdir_stage.outs[0].cache_path)
    remove(subdir_stage.outs[0].fs_path)

    result = dvc.plots.show()
    assert get_plot(result, "workspace", typ="definitions", file="", endkey="error")
    # make sure others gets loaded
    assert get_plot(result, "workspace", file="plot.json") == metric


@pytest.mark.parametrize(
    "plot_config,expected_datafiles",
    [
        (
            {
                "comparison": {
                    "x": {"data1.json": "a"},
                    "y": {"sub/dir/data2.json": "b"},
                }
            },
            ["data1.json", os.path.join("sub", "dir", "data2.json")],
        ),
        (
            {"data1.json": {"x": "c", "y": "a", "title": "File as key test"}},
            ["data1.json"],
        ),
        (
            {
                "infer_data_from_y": {
                    "x": "a",
                    "y": {"data1.json": "b", "sub/dir/data2.json": "c"},
                }
            },
            ["data1.json", os.path.join("sub", "dir", "data2.json")],
        ),
    ],
)
def test_top_level_plots(tmp_dir, dvc, plot_config, expected_datafiles):
    data = {
        "data1.json": [
            {"a": 1, "b": 0.1, "c": 0.01},
            {"a": 2, "b": 0.2, "c": 0.02},
        ],
        os.path.join("sub", "dir", "data.json"): [
            {"a": 6, "b": 0.6, "c": 0.06},
            {"a": 7, "b": 0.7, "c": 0.07},
        ],
    }

    for filename, content in data.items():
        dirname = os.path.dirname(filename)
        if dirname:
            os.makedirs(dirname)
        (tmp_dir / filename).dump_json(content, sort_keys=True)

    config_file = "dvc.yaml"
    with modify_yaml(config_file) as dvcfile_content:
        dvcfile_content["plots"] = [plot_config]

    result = dvc.plots.show()

    assert plot_config == get_plot(
        result, "workspace", typ="definitions", file=config_file
    )

    for filename, content in data.items():
        if filename in expected_datafiles:
            assert content == get_plot(result, "workspace", file=filename)
        else:
            assert filename not in get_plot(result, "workspace")


def test_show_plots_defined_with_native_os_path(tmp_dir, dvc, scm, capsys):
    """Regression test for #8689"""
    top_level_plot = os.path.join("subdir", "top_level_plot.csv")
    stage_plot = os.path.join("subdir", "stage_plot.csv")
    (tmp_dir / "subdir").mkdir()
    (tmp_dir / top_level_plot).write_text("foo,bar\n1,2")
    (tmp_dir / stage_plot).write_text("foo,bar\n1,2")
    (tmp_dir / "dvc.yaml").dump({"plots": [top_level_plot]})

    dvc.stage.add(name="foo", plots=[stage_plot], cmd="echo foo")

    plots = dvc.plots.show()

    # sources are in posixpath format
    sources = plots["workspace"]["sources"]["data"]
    assert sources["subdir/top_level_plot.csv"]["data"] == [{"foo": "1", "bar": "2"}]
    assert sources["subdir/stage_plot.csv"]["data"] == [{"foo": "1", "bar": "2"}]
    # definitions are in native os format
    definitions = plots["workspace"]["definitions"]["data"]
    assert top_level_plot in definitions["dvc.yaml"]["data"]
    assert stage_plot in definitions[""]["data"]

    capsys.readouterr()
    assert main(["plots", "show", "--json"]) == 0
    out, _ = capsys.readouterr()
    json_out = json.loads(out)
    assert "errors" not in json_out

    json_data = json_out["data"]
    assert json_data[f"{top_level_plot}"]
    assert json_data[stage_plot]


@pytest.mark.parametrize(
    "plot_config,expanded_config,expected_datafiles",
    [
        (
            {
                "comparison": {
                    "x": {"${data1}": "${a}"},
                    "y": {"sub/dir/data2.json": "${b}"},
                }
            },
            {
                "comparison": {
                    "x": {"data1.json": "a"},
                    "y": {"sub/dir/data2.json": "b"},
                }
            },
            ["data1.json", os.path.join("sub", "dir", "data2.json")],
        ),
        (
            {"${data1}": None},
            {"data1.json": {}},
            ["data1.json"],
        ),
        (
            "${data1}",
            {"data1.json": {}},
            ["data1.json"],
        ),
    ],
)
def test_top_level_parametrized(
    tmp_dir, dvc, plot_config, expanded_config, expected_datafiles
):
    (tmp_dir / "params.yaml").dump(
        {"data1": "data1.json", "a": "a", "b": "b", "c": "c"}
    )
    data = {
        "data1.json": [
            {"a": 1, "b": 0.1, "c": 0.01},
            {"a": 2, "b": 0.2, "c": 0.02},
        ],
        os.path.join("sub", "dir", "data.json"): [
            {"a": 6, "b": 0.6, "c": 0.06},
            {"a": 7, "b": 0.7, "c": 0.07},
        ],
    }

    for filename, content in data.items():
        dirname = os.path.dirname(filename)
        if dirname:
            os.makedirs(dirname)
        (tmp_dir / filename).dump_json(content, sort_keys=True)

    config_file = "dvc.yaml"
    with modify_yaml(config_file) as dvcfile_content:
        dvcfile_content["plots"] = [plot_config]

    result = dvc.plots.show()

    assert expanded_config == get_plot(
        result, "workspace", typ="definitions", file=config_file
    )

    for filename, content in data.items():
        if filename in expected_datafiles:
            assert content == get_plot(result, "workspace", file=filename)
        else:
            assert filename not in get_plot(result, "workspace")
