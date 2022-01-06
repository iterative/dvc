import textwrap

import pytest

from dvc.cli import parse_args
from dvc.command.note import CmdNote


@pytest.mark.parametrize("paths", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_set(mocker, paths):
    args = ["note", "set", "-K", "color", "-V", "green"]
    cli_args = parse_args(args + paths)
    assert cli_args.func == CmdNote
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(
        action="set", targets=paths, key="color", value="green"
    )


@pytest.mark.parametrize("paths", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_find(mocker, paths):
    args = ["note", "find", "-K", "color"] + paths
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote

    cmd = cli_args.func(cli_args)

    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(
        action="find", targets=paths, key="color", value=None
    )


def test_note_find_output_single(mocker, capsys):
    args = ["note", "find", "-K", "color", ["a.txt"]]
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote

    cmd = cli_args.func(cli_args)
    result = [["a.txt", "color", "green"]]
    mocker.patch("dvc.repo.Repo.note", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    expected = textwrap.dedent(
        """\
        green
        """
    )
    assert out == expected


def test_note_find_output_multi(mocker, capsys):
    args = ["note", "find", "-K", "color", ["a.txt", "b.txt"]]
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote

    cmd = cli_args.func(cli_args)
    result = [["a.txt", "color", "green"], ["b.txt", "color", "blue"]]
    mocker.patch("dvc.repo.Repo.note", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert out == textwrap.dedent(
        """\
        a.txt: green
        b.txt: blue
        """
    )


@pytest.mark.parametrize("paths", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_list(mocker, paths):
    args = ["note", "list"] + paths
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(
        action="list", targets=paths, key=None, value=None
    )


def test_note_list_output_single(mocker, capsys):
    args = ["note", "list", ["a.txt"]]
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote

    cmd = cli_args.func(cli_args)
    result = [["a.txt", ["color", "size"]]]
    mocker.patch("dvc.repo.Repo.note", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    expected = textwrap.dedent(
        """\
        color
        size
        """
    )
    assert out == expected


def test_note_list_output_multi(mocker, capsys):
    args = ["note", "list", ["a.txt", "b.txt"]]
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote

    cmd = cli_args.func(cli_args)
    result = [["a.txt", ["color", "size"]], ["b.txt", ["size", "flavor"]]]
    mocker.patch("dvc.repo.Repo.note", return_value=result)

    assert cmd.run() == 0
    out, _ = capsys.readouterr()
    assert out == textwrap.dedent(
        """\
        a.txt
        - color
        - size
        b.txt
        - flavor
        - size
        """
    )


@pytest.mark.parametrize("paths", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_remove(mocker, paths):
    args = ["note", "remove", "-K", "color"] + paths
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(
        action="remove", targets=paths, key="color", value=None
    )
