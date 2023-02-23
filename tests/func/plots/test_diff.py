import pytest

from tests.utils.plots import get_plot


def test_diff_dirty(tmp_dir, scm, dvc, run_copy_metrics):
    (tmp_dir / "metric_t.json").dump([{"y": 2}, {"y": 3}], sort_keys=True)
    run_copy_metrics(
        "metric_t.json",
        "metric.json",
        plots=["metric.json"],
        name="train",
        commit="init",
    )

    metric_head = [{"y": 3}, {"y": 5}]
    (tmp_dir / "metric_t.json").dump_json(metric_head, sort_keys=True)
    dvc.reproduce()
    scm.add(["dvc.lock"])
    scm.commit("second")

    metric_1 = [{"y": 5}, {"y": 6}]
    (tmp_dir / "metric_t.json").dump_json(metric_1, sort_keys=True)
    dvc.reproduce()

    props = {"fields": ["y"]}
    diff_result = dvc.plots.diff(props=props)

    assert get_plot(diff_result, "workspace", file="metric.json") == metric_1
    assert get_plot(
        diff_result, "workspace", "definitions", file="", endkey="data"
    ) == {"metric.json": props}
    assert get_plot(diff_result, "HEAD", file="metric.json") == metric_head
    assert get_plot(diff_result, "HEAD", "definitions", file="", endkey="data") == {
        "metric.json": props
    }

    metric_2 = [{"y": 7}, {"y": 8}]
    (tmp_dir / "metric.json").dump_json(metric_2, sort_keys=True)

    diff_result = dvc.plots.diff(props=props)
    assert get_plot(diff_result, "workspace", file="metric.json") == metric_2
    assert get_plot(
        diff_result, "workspace", "definitions", file="", endkey="data"
    ) == {"metric.json": props}

    assert get_plot(diff_result, "HEAD", file="metric.json") == metric_head
    assert get_plot(
        diff_result, "workspace", "definitions", file="", endkey="data"
    ) == {"metric.json": props}


@pytest.mark.vscode
def test_no_commits(tmp_dir):
    from dvc.repo import Repo
    from dvc.scm import Git

    git = Git.init(tmp_dir.fs_path)
    assert git.no_commits

    assert Repo.init().plots.diff() == {}
