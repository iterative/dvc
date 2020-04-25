from dvc.cli import parse_args
from dvc.command.metrics import CmdMetricsDiff, _show_diff, CmdMetricsShow


def test_metrics_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "metrics",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "-R",
            "--all",
            "--show-json",
            "--targets",
            "target1",
            "target2",
        ]
    )
    assert cli_args.func == CmdMetricsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.metrics.diff.diff", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        targets=["target1", "target2"],
        a_rev="HEAD~10",
        b_rev="HEAD~1",
        recursive=True,
        all=True,
    )


def test_metrics_show_json_diff():
    assert _show_diff(
        {"metrics.json": {"a.b.c": {"old": 1, "new": 2, "diff": 3}}}
    ) == (
        "    Path       Metric   Value   Change\n"
        "metrics.json   a.b.c    2       3     "
    )


def test_metrics_show_raw_diff():
    assert _show_diff({"metrics": {"": {"old": "1", "new": "2"}}}) == (
        " Path     Metric   Value         Change      \n"
        "metrics            2       diff not supported"
    )


def test_metrics_diff_no_diff():
    assert _show_diff(
        {"other.json": {"a.b.d": {"old": "old", "new": "new"}}}
    ) == (
        "   Path      Metric   Value         Change      \n"
        "other.json   a.b.d    new     diff not supported"
    )


def test_metrics_diff_no_changes():
    assert _show_diff({}) == ""


def test_metrics_diff_new_metric():
    assert _show_diff(
        {"other.json": {"a.b.d": {"old": None, "new": "new"}}}
    ) == (
        "   Path      Metric   Value         Change      \n"
        "other.json   a.b.d    new     diff not supported"
    )


def test_metrics_diff_deleted_metric():
    assert _show_diff(
        {"other.json": {"a.b.d": {"old": "old", "new": None}}}
    ) == (
        "   Path      Metric   Value         Change      \n"
        "other.json   a.b.d    None    diff not supported"
    )


def test_metrics_show(dvc, mocker):
    cli_args = parse_args(
        [
            "metrics",
            "show",
            "-R",
            "--all-tags",
            "--all-branches",
            "--all-commits",
            "target1",
            "target2",
        ]
    )
    assert cli_args.func == CmdMetricsShow

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.metrics.show.show", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo,
        ["target1", "target2"],
        recursive=True,
        all_tags=True,
        all_branches=True,
        all_commits=True,
    )


def test_metrics_diff_prec():
    assert _show_diff(
        {"other.json": {"a.b": {"old": 0.0042, "new": 0.0043, "diff": 0.0001}}}
    ) == (
        "   Path      Metric   Value    Change\n"
        "other.json   a.b      0.0043   0.0001"
    )


def test_metrics_diff_sorted():
    assert _show_diff(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6, "diff": 1},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        }
    ) == (
        "    Path       Metric   Value   Change\n"
        "metrics.yaml   a.b.c    2       1     \n"
        "metrics.yaml   a.d.e    4       1     \n"
        "metrics.yaml   x.b      6       1     "
    )
