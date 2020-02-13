from dvc.cli import parse_args
from dvc.command.metrics import CmdMetricsDiff, _show_diff


def test_metrics_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "metrics",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "-t",
            "json",
            "-x",
            "x.path",
            "-R",
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
        typ="json",
        xpath="x.path",
        recursive=True,
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
    assert _show_diff({}) == "No changes."
