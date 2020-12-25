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
            "--show-md",
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
        Revision    Path          a    b.ad    b.bc
        branch_1    metrics.json  0    0.0     0.0"""
    )


def test_metrics_show_with_no_revision():
    assert _show_metrics(
        {"branch_1": {"metrics.json": {"a": 0, "b": {"ad": 0.0, "bc": 0.0}}}},
        all_branches=False,
    ) == textwrap.dedent(
        """\
        Path          a    b.ad    b.bc
        metrics.json  0    0.0     0.0"""
    )


def test_metrics_show_with_non_dict_values():
    assert _show_metrics(
        {"branch_1": {"metrics.json": 1}}, all_branches=True,
    ) == textwrap.dedent(
        """\
        Revision    Path
        branch_1    metrics.json  1"""
    )


def test_metrics_show_with_multiple_revision():
    assert _show_metrics(
        {
            "branch_1": {"metrics.json": {"a": 1, "b": {"ad": 1, "bc": 2}}},
            "branch_2": {"metrics.json": {"a": 1, "b": {"ad": 3, "bc": 4}}},
        },
        all_branches=True,
    ) == textwrap.dedent(
        """\
        Revision    Path          a    b.ad    b.bc
        branch_1    metrics.json  1    1       2
        branch_2    metrics.json  1    3       4"""
    )


def test_metrics_show_with_one_revision_multiple_paths():
    assert _show_metrics(
        {
            "branch_1": {
                "metrics.json": {"a": 1, "b": {"ad": 0.1, "bc": 1.03}},
                "metrics_1.json": {"a": 2.3, "b": {"ad": 6.5, "bc": 7.9}},
            }
        },
        all_branches=True,
    ) == textwrap.dedent(
        """\
        Revision    Path            a    b.ad    b.bc
        branch_1    metrics.json    1    0.1     1.03
        branch_1    metrics_1.json  2.3  6.5     7.9"""
    )


def test_metrics_show_with_different_metrics_header():
    assert _show_metrics(
        {
            "branch_1": {"metrics.json": {"b": {"ad": 1, "bc": 2}, "c": 4}},
            "branch_2": {"metrics.json": {"a": 1, "b": {"ad": 3, "bc": 4}}},
        },
        all_branches=True,
    ) == textwrap.dedent(
        """\
        Revision    Path          a    b.ad    b.bc    c
        branch_1    metrics.json  —    1       2       4
        branch_2    metrics.json  1    3       4       —"""
    )


def test_metrics_show_precision():
    metrics = {
        "branch_1": {
            "metrics.json": {
                "a": 1.098765366365355,
                "b": {"ad": 1.5342673, "bc": 2.987725527},
            }
        }
    }

    assert _show_metrics(metrics, all_branches=True,) == textwrap.dedent(
        """\
        Revision    Path          a        b.ad     b.bc
        branch_1    metrics.json  1.09877  1.53427  2.98773"""
    )

    assert _show_metrics(
        metrics, all_branches=True, precision=4
    ) == textwrap.dedent(
        """\
        Revision    Path          a       b.ad    b.bc
        branch_1    metrics.json  1.0988  1.5343  2.9877"""
    )

    assert _show_metrics(
        metrics, all_branches=True, precision=7
    ) == textwrap.dedent(
        """\
        Revision    Path          a          b.ad       b.bc
        branch_1    metrics.json  1.0987654  1.5342673  2.9877255"""
    )


def test_metrics_show_default():
    assert _show_metrics(
        {
            "metrics.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4, "diff": 1},
                "a.b.c": {"old": 1, "new": 2, "diff": 1},
            }
        },
    ) == textwrap.dedent(
        """\
        Path    diff    new    old
        x.b     —       6      5
        a.d.e   1       4      3
        a.b.c   1       2      1"""
    )


def test_metrics_show_md():
    assert _show_metrics(
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
        | Path   | diff   | new   | old   |
        |--------|--------|-------|-------|
        | x.b    | —      | 6     | 5     |
        | a.d.e  | 1      | 4     | 3     |
        | a.b.c  | 1      | 2     | 1     |
        """
    )
