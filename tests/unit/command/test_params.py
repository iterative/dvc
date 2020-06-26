import logging
import textwrap

from dvc.cli import parse_args
from dvc.command.params import CmdParamsDiff, _show_diff


def test_params_diff(dvc, mocker):
    cli_args = parse_args(
        [
            "params",
            "diff",
            "HEAD~10",
            "HEAD~1",
            "--all",
            "--show-json",
            "--show-md",
            "--no-path",
        ]
    )
    assert cli_args.func == CmdParamsDiff

    cmd = cli_args.func(cli_args)
    m = mocker.patch("dvc.repo.params.diff.diff", return_value={})

    assert cmd.run() == 0

    m.assert_called_once_with(
        cmd.repo, a_rev="HEAD~10", b_rev="HEAD~1", all=True,
    )


def test_params_diff_changed():
    assert _show_diff(
        {"params.yaml": {"a.b.c": {"old": 1, "new": 2}}}
    ) == textwrap.dedent(
        """\
        Path         Param    Old    New
        params.yaml  a.b.c    1      2"""
    )


def test_params_diff_list():
    assert _show_diff(
        {"params.yaml": {"a.b.c": {"old": 1, "new": [2, 3]}}}
    ) == textwrap.dedent(
        """\
        Path         Param    Old    New
        params.yaml  a.b.c    1      [2, 3]"""
    )


def test_params_diff_unchanged():
    assert _show_diff(
        {"params.yaml": {"a.b.d": {"old": "old", "new": "new"}}}
    ) == textwrap.dedent(
        """\
        Path         Param    Old    New
        params.yaml  a.b.d    old    new"""
    )


def test_params_diff_no_changes():
    assert _show_diff({}) == ""


def test_params_diff_new():
    assert _show_diff(
        {"params.yaml": {"a.b.d": {"old": None, "new": "new"}}}
    ) == textwrap.dedent(
        """\
        Path         Param    Old    New
        params.yaml  a.b.d    None   new"""
    )


def test_params_diff_deleted():
    assert _show_diff(
        {"params.yaml": {"a.b.d": {"old": "old", "new": None}}}
    ) == textwrap.dedent(
        """\
        Path         Param    Old    New
        params.yaml  a.b.d    old    None"""
    )


def test_params_diff_prec():
    assert _show_diff(
        {"params.yaml": {"train.lr": {"old": 0.0042, "new": 0.0043}}}
    ) == textwrap.dedent(
        """\
        Path         Param     Old     New
        params.yaml  train.lr  0.0042  0.0043"""
    )


def test_params_diff_show_json(dvc, mocker, caplog):
    cli_args = parse_args(
        ["params", "diff", "HEAD~10", "HEAD~1", "--show-json"]
    )
    cmd = cli_args.func(cli_args)
    mocker.patch(
        "dvc.repo.params.diff.diff", return_value={"params.yaml": {"a": "b"}}
    )
    with caplog.at_level(logging.INFO, logger="dvc"):
        assert cmd.run() == 0
        assert '{"params.yaml": {"a": "b"}}\n' in caplog.text


def test_params_diff_sorted():
    assert _show_diff(
        {
            "params.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4},
                "a.b.c": {"old": 1, "new": 2},
            }
        }
    ) == textwrap.dedent(
        """\
        Path         Param    Old    New
        params.yaml  a.b.c    1      2
        params.yaml  a.d.e    3      4
        params.yaml  x.b      5      6"""
    )


def test_params_diff_markdown_empty():
    assert _show_diff({}, markdown=True) == textwrap.dedent(
        """\
        | Path   | Param   | Old   | New   |
        |--------|---------|-------|-------|
        """
    )


def test_params_diff_markdown():
    assert _show_diff(
        {
            "params.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": None, "new": 4},
                "a.b.c": {"old": 1, "new": None},
            }
        },
        markdown=True,
    ) == textwrap.dedent(
        """\
        | Path        | Param   | Old   | New   |
        |-------------|---------|-------|-------|
        | params.yaml | a.b.c   | 1     | None  |
        | params.yaml | a.d.e   | None  | 4     |
        | params.yaml | x.b     | 5     | 6     |
        """
    )


def test_params_diff_no_path():
    assert _show_diff(
        {
            "params.yaml": {
                "x.b": {"old": 5, "new": 6},
                "a.d.e": {"old": 3, "new": 4},
                "a.b.c": {"old": 1, "new": 2},
            }
        },
        no_path=True,
    ) == textwrap.dedent(
        """\
        Param    Old    New
        a.b.c    1      2
        a.d.e    3      4
        x.b      5      6"""
    )
