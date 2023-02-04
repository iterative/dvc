import textwrap

import pytest
from rich.style import Style

from dvc.ui import ui


def test_plain(capsys):
    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
    )
    captured = capsys.readouterr()
    assert captured.out == textwrap.dedent(
        """\
        first    second
        foo      bar
        foo1     bar1
        foo2     bar2
    """
    )


def test_plain_md(capsys):
    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
        markdown=True,
    )
    captured = capsys.readouterr()
    assert captured.out == textwrap.dedent(
        """\
        | first   | second   |
        |---------|----------|
        | foo     | bar      |
        | foo1    | bar1     |
        | foo2    | bar2     |\n
    """
    )


def test_plain_pager(mocker):
    pager_mock = mocker.patch("dvc.ui.pager.pager")
    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
        pager=True,
    )

    pager_mock.assert_called_once_with(
        textwrap.dedent(
            """\
            first    second
            foo      bar
            foo1     bar1
            foo2     bar2
            """
        )
    )


def test_plain_headerless(capsys):
    ui.table([("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")])
    captured = capsys.readouterr()
    assert captured.out == textwrap.dedent(
        """\
        foo   bar
        foo1  bar1
        foo2  bar2
    """
    )


def test_rich_simple(capsys):
    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
        rich_table=True,
    )
    # not able to test the actual style for now
    captured = capsys.readouterr()
    assert [row.strip() for row in captured.out.splitlines() if row.strip()] == [
        "first  second",
        "foo    bar",
        "foo1   bar1",
        "foo2   bar2",
    ]


def test_rich_headerless(capsys):
    ui.table([("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")], rich_table=True)
    captured = capsys.readouterr()
    assert [row.strip() for row in captured.out.splitlines() if row.strip()] == [
        "foo   bar",
        "foo1  bar1",
        "foo2  bar2",
    ]


def test_rich_border(capsys):
    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
        rich_table=True,
        borders="simple",
    )
    captured = capsys.readouterr()
    assert [row.strip() for row in captured.out.splitlines() if row.strip()] == [
        "first   second",
        "────────────────",
        "foo     bar",
        "foo1    bar1",
        "foo2    bar2",
    ]


@pytest.mark.parametrize(
    "extra_opts",
    [
        {"header_styles": [{"style": Style(bold=True)}]},
        {"header_styles": {"first": {"style": Style(bold=True)}}},
        {"row_styles": [{"style": Style(bold=True)}]},
    ],
)
def test_rich_styles(capsys, extra_opts):
    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
        rich_table=True,
        **extra_opts,
    )
    # not able to test the actual style for now
    captured = capsys.readouterr()
    assert [row.strip() for row in captured.out.splitlines() if row.strip()] == [
        "first  second",
        "foo    bar",
        "foo1   bar1",
        "foo2   bar2",
    ]


def test_rich_pager(mocker):
    pager_mock = mocker.patch("dvc.ui.pager.pager")

    ui.table(
        [("foo", "bar"), ("foo1", "bar1"), ("foo2", "bar2")],
        headers=["first", "second"],
        rich_table=True,
        pager=True,
    )
    received_text = pager_mock.call_args[0][0]
    assert [row.strip() for row in received_text.splitlines() if row.strip()] == [
        "first  second",
        "foo    bar",
        "foo1   bar1",
        "foo2   bar2",
    ]


@pytest.mark.parametrize("rich_table", [True, False])
def test_empty(capsys, rich_table):
    ui.table([], rich_table=rich_table)
    out, err = capsys.readouterr()
    assert (out, err) == ("", "")


def test_empty_markdown(capsys):
    ui.table([], headers=["Col1", "Col2"], markdown=True)
    out, err = capsys.readouterr()
    assert (out, err) == ("| Col1   | Col2   |\n|--------|--------|\n\n", "")
