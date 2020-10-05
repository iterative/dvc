import textwrap

from dvc.cli import parse_args
from dvc.command.metrics import (
    CmdMetricsDiff,
    CmdMetricsShow,
    _show_diff,
    _show_metrics,
)


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
            "--show-md",
            "--no-path",
            "--precision",
            "10",
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
    ) == textwrap.dedent(
        """\
        Path          Metric    Old    New    Change
        metrics.json  a.b.c     1      2      3"""
    )


def test_metrics_show_raw_diff():
    assert _show_diff(
        {"metrics": {"": {"old": "1", "new": "2"}}}
    ) == textwrap.dedent(
        """\
        Path     Metric    Old    New    Change
        metrics            1      2      —"""
    )


def test_metrics_diff_no_diff():
    assert _show_diff(
        {"other.json": {"a.b.d": {"old": "old", "new": "new"}}}
    ) == textwrap.dedent(
        """\
        Path        Metric    Old    New    Change
        other.json  a.b.d     old    new    —"""
    )


def test_metrics_diff_no_changes():
    assert _show_diff({}) == ""


def test_metrics_diff_new_metric():
    assert _show_diff(
        {"other.json": {"a.b.d": {"old": None, "new": "new"}}}
    ) == textwrap.dedent(
        """\
        Path        Metric    Old    New    Change
        other.json  a.b.d     —      new    —"""
    )


def test_metrics_diff_deleted_metric():
    assert _show_diff(
        {"other.json": {"a.b.d": {"old": "old", "new": None}}}
    ) == textwrap.dedent(
        """\
        Path        Metric    Old    New    Change
        other.json  a.b.d     old    —      —"""
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


def test_metrics_diff_precision():
    diff = {
        "other.json": {
            "a.b": {
                "old": 0.1234567,
                "new": 0.765432101234567,
                "diff": 0.641975401234567,
            }
        }
    }

    assert _show_diff(diff) == textwrap.dedent(
        """\
        Path        Metric    Old      New      Change
        other.json  a.b       0.12346  0.76543  0.64198"""
    )

    assert _show_diff(diff, precision=10) == textwrap.dedent(
        """\
        Path        Metric    Old        New           Change
        other.json  a.b       0.1234567  0.7654321012  0.6419754012"""
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
    ) == textwrap.dedent(
        """\
        Path          Metric    Old    New    Change
        metrics.yaml  a.b.c     1      2      1
        metrics.yaml  a.d.e     3      4      1
        metrics.yaml  x.b       5      6      1"""
    )


def test_metrics_diff_markdown_empty():
    assert _show_diff({}, markdown=True) == textwrap.dedent(
        """\
        | Path   | Metric   | Old   | New   | Change   |
        |--------|----------|-------|-------|----------|
        """
    )


def test_metrics_diff_markdown():
    assert _show_diff(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
        markdown=True,
    ) == textwrap.dedent(
        """\
        | Path         | Metric   | Old   | New   | Change   |
        |--------------|----------|-------|-------|----------|
        | metrics.yaml | a.b.c    | 1     | 2     | 1        |
        | metrics.yaml | a.d.e    | 3     | 4     | 1        |
        | metrics.yaml | x.b      | 5     | 6     | —        |
        """
    )


def test_metrics_diff_no_path():
    assert _show_diff(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6, "diff": 1},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
        no_path=True,
    ) == textwrap.dedent(
        """\
        Metric    Old    New    Change
        a.b.c     1      2      1
        a.d.e     3      4      1
        x.b       5      6      1"""
    )


def test_metrics_show_with_valid_falsey_values():
    assert _show_metrics(
        {"branch_1": {"metrics.json": {"a": 0, "b": {"ad": 0.0, "bc": 0.0}}}},
        all_branches=True,
    ) == textwrap.dedent(
        """\
        branch_1:
        \tmetrics.json:
        \t\ta: 0
        \t\tb.ad: 0.0
        \t\tb.bc: 0.0"""
    )
