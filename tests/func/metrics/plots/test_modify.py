import pytest

from dvc.dvcfile import PIPELINE_LOCK
from dvc.repo.plots import PropsNotFoundError
from dvc.repo.plots.template import TemplateNotFoundError
from dvc.utils import relpath
from tests.func.metrics.utils import _write_json


def test_plots_modify_existing_template(
    tmp_dir, dvc, run_copy_metrics, custom_template
):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    stage = run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics",
        single_stage=False,
    )
    dvc.plots.modify(
        "metric.json", props={"template": relpath(custom_template)}
    )
    stage = stage.reload()
    assert stage.outs[0].plot == {"template": relpath(custom_template)}


def test_plots_modify_should_not_change_lockfile(
    tmp_dir, dvc, run_copy_metrics, custom_template
):
    _write_json(tmp_dir, [{"a": 1, "b": 2}], "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics",
        single_stage=False,
    )

    (tmp_dir / PIPELINE_LOCK).unlink()
    dvc.plots.modify(
        "metric.json", props={"template": relpath(custom_template)}
    )
    assert not (tmp_dir / PIPELINE_LOCK).exists()


def test_plots_modify_not_existing_template(dvc):
    with pytest.raises(TemplateNotFoundError):
        dvc.plots.modify(
            "metric.json", props={"template": "not-existing-template.json"}
        )


def test_unset_nonexistent(tmp_dir, dvc, run_copy_metrics, custom_template):
    metric = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
    _write_json(tmp_dir, metric, "metric_t.json")
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots_no_cache=["metric.json"],
        name="copy-metrics",
        single_stage=False,
    )

    with pytest.raises(PropsNotFoundError):
        dvc.plots.modify(
            "metric.json", unset=["nonexistent"],
        )
