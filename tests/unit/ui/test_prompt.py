import io
from unittest.mock import call

import pytest
from rich.console import Console as RichConsole

from dvc.ui.prompt import Confirm, InvalidResponse, Prompt

# pylint: disable=no-member


@pytest.mark.parametrize("side_effect", [EOFError, KeyboardInterrupt])
@pytest.mark.parametrize("prompt_cls", [Prompt, Confirm])
def test_prompt_interrupt_message_not_interleaved(
    prompt_cls, side_effect, mocker
):
    """Test that the interrupt message is not interleaved with the prompt."""
    prompt = "should print exception in the next line"
    console = RichConsole(file=io.StringIO())

    with pytest.raises(side_effect):
        mocker.patch.object(console, "input", side_effect=side_effect)
        prompt_cls.ask(prompt=prompt, console=console)
    # as we are raising side_effect on the input, unfortunately we cannot
    # assert for the prompt string, only the newline.
    assert console.file.getvalue() == "\n"


def test_prompt_str():
    console = RichConsole(file=io.StringIO())
    assert (
        Prompt.ask(
            "What is your name?", console=console, stream=io.StringIO("foo")
        )
        == "foo"
    )
    assert console.file.getvalue() == "What is your name?: "


def test_prompt_disallows_empty_response(mocker):
    console = RichConsole(file=io.StringIO())
    Prompt.prompt_(
        "What is your name?",
        console=console,
        stream=io.StringIO("\nfoo"),
    )
    expected = (
        "What is your name?: Response required. Please try again.\n"
        "What is your name?: "
    )
    assert console.file.getvalue() == expected


@pytest.mark.parametrize(
    "default, brack",
    [
        ("default", "[default, n to omit]"),
        (None, "[n to omit]"),
    ],
)
def test_prompt_with_conditional_skip(mocker, default, brack):
    console = RichConsole(file=io.StringIO())
    mocked_validator = mocker.MagicMock(return_value="something")
    assert (
        Prompt.prompt_(
            "What is your name?",
            console=console,
            allow_omission=True,
            stream=io.StringIO("n\n"),
            default=default,
            validator=mocked_validator,
        )
        is None
    )
    assert console.file.getvalue() == f"What is your name? {brack}: "
    mocked_validator.assert_not_called()


def test_prompt_retries_on_invalid_response_from_validator(mocker):
    console = RichConsole(file=io.StringIO())
    mocked_validator = mocker.MagicMock(
        side_effect=[InvalidResponse("it is a number"), "foo"]
    )
    assert (
        Prompt.ask(
            "what is your name?",
            console=console,
            stream=io.StringIO("3\nfoo"),
            validator=mocked_validator,
        )
        == "foo"
    )
    mocked_validator.assert_has_calls([call("3"), call("foo")])

    expected = "what is your name?: it is a number\nwhat is your name?: "
    assert console.file.getvalue() == expected


def test_prompt_shows_message_from_validator_response(mocker):
    console = RichConsole(file=io.StringIO())
    mocked_validator = mocker.MagicMock(
        return_value=("foo@email", "failed to send a verification email")
    )
    assert (
        Prompt.ask(
            "what is your email?",
            console=console,
            stream=io.StringIO("foo@email"),
            validator=mocked_validator,
        )
        == "foo@email"
    )
    mocked_validator.assert_has_calls([call("foo@email")])

    expected = "what is your email?: failed to send a verification email\n"
    assert console.file.getvalue() == expected


def test_prompt_default(mocker):
    console = RichConsole(file=io.StringIO())
    mocked_validator = mocker.MagicMock(return_value="default")
    assert (
        Prompt.ask(
            "What is your name?",
            console=console,
            stream=io.StringIO(""),
            default="default",
            validator=mocked_validator,
        )
        == "default"
    )
    assert console.file.getvalue() == "What is your name? [default]: "
    mocked_validator.assert_has_calls([call("default")])


@pytest.mark.parametrize(
    "default, inner", [(True, "Y/n"), (False, "y/N"), (Ellipsis, "y/n")]
)
def test_prompt_confirm(default, inner):
    console = RichConsole(file=io.StringIO())
    assert (
        Confirm.ask(
            "exit?",
            console=console,
            stream=io.StringIO("y\n"),
            default=default,
        )
        is True
    )
    output = console.file.getvalue()
    assert output == f"exit? [{inner}]: "
