import pytest
import textwrap

from dvc.cli import parse_args
from dvc.command.note import CmdNote


@pytest.mark.parametrize("filenames", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_set(mocker, filenames):
    cli_args = parse_args(["note", "set", "-K", "color", "-V", "green"] + filenames)
    assert cli_args.func == CmdNote
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(action="set", targets=filenames, key="color", value="green")


@pytest.mark.parametrize("filenames", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_find(mocker, filenames):
    args = ["note", "find", "-K", "color"] + filenames
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote

    cmd = cli_args.func(cli_args)

    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(action="find", targets=filenames, key="color", value=None)


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


@pytest.mark.parametrize("filenames", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_list(mocker, filenames):
    args = ["note", "list"] + filenames
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(action="list", targets=filenames, key=None, value=None)


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


@pytest.mark.parametrize("filenames", [["a.txt"], ["a.txt", "b.txt", "c.txt"]])
def test_note_remove(mocker, filenames):
    args = ["note", "remove", "-K", "color"] + filenames
    cli_args = parse_args(args)
    assert cli_args.func == CmdNote
    cmd = cli_args.func(cli_args)
    m = mocker.patch.object(cmd.repo, "note", autospec=True)
    assert cmd.run() == 0
    m.assert_called_once_with(action="remove", targets=filenames, key="color", value=None)
