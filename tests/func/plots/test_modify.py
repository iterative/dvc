import pytest

from dvc.dvcfile import LOCK_FILE
from dvc.repo.plots import PropsNotFoundError
from dvc.utils import relpath
from tests.utils.plots import get_plot


def test_plots_modify_existing_template(
    tmp_dir, dvc, run_copy_metrics, custom_template
):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    (tmp_dir / "metric_t.json").dump_json(metric, sort_keys=True)
    stage = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics",
        single_stage=False,
    )
    dvc.plots.modify("metric.json", props={"template": relpath(custom_template)})
    stage = stage.reload()
    assert stage.outs[0].plot == {"template": relpath(custom_template)}


def test_plots_modify_should_not_change_lockfile(
    tmp_dir, dvc, run_copy_metrics, custom_template
):
    (tmp_dir / "metric_t.json").dump_json([{"a": 1, "b": 2}], sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics",
        single_stage=False,
    )

    (tmp_dir / LOCK_FILE).unlink()
    dvc.plots.modify("metric.json", props={"template": relpath(custom_template)})
    assert not (tmp_dir / LOCK_FILE).exists()


def test_plots_modify_not_existing_template(dvc):
    from dvc_render.vega_templates import TemplateNotFoundError

    with pytest.raises(TemplateNotFoundError):
        dvc.plots.modify(
            "metric.json", props={"template": "not-existing-template.json"}
        )


def test_unset_nonexistent(tmp_dir, dvc, run_copy_metrics, custom_template):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    (tmp_dir / "metric_t.json").dump_json(metric, sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics",
        single_stage=False,
    )

    with pytest.raises(PropsNotFoundError):
        dvc.plots.modify("metric.json", unset=["nonexistent"])


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
    dvc.plots.modify("subdir", {"title": "TITLE"})

    result = dvc.plots.show()
    assert get_plot(result, "workspace", typ="definitions", file="") == {
        p1: {"title": "TITLE"},
        p2: {"title": "TITLE"},
    }
