import datetime
import textwrap

import pytest

from dvc.ui import Console


def test_write(capsys):
    """Test that ui.write works."""
    console = Console(enable=True)
    message = "hello world"
    console.write(message)
    console.error_write(message)

    captured = capsys.readouterr()
    assert captured.out == f"{message}\n"
    assert captured.err == f"{message}\n"


@pytest.mark.parametrize(
    "isatty, expected_output",
    [
        (
            True,
            textwrap.dedent(
                """\
        {
          "hello": "world",
          "date": "1970-01-01 00:00:00"
        }
    """
            ),
        ),
        (
            False,
            textwrap.dedent(
                """\
        {"hello": "world", "date": "1970-01-01 00:00:00"}
        """
            ),
        ),
    ],
)
def test_write_json(capsys, mocker, isatty, expected_output):
    """Test that ui.write_json works."""

    console = Console(enable=True)
    mocker.patch.object(console, "isatty", return_value=isatty)
    message = {"hello": "world", "date": datetime.datetime(1970, 1, 1)}
    console.write_json(message, default=str)
    captured = capsys.readouterr()
    assert captured.out == expected_output


def test_capsys_works(capsys):
    """Sanity check that capsys can capture outputs from a global ui."""
    from dvc.ui import ui

    message = "hello world"
    ui.write(message)
    ui.error_write(message)

    captured = capsys.readouterr()
    assert captured.out == f"{message}\n"
    assert captured.err == f"{message}\n"
