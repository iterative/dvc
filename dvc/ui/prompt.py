from typing import TYPE_CHECKING, Any, Callable, Optional, Tuple, Union

import rich.prompt

if TYPE_CHECKING:
    from rich.console import Console as RichConsole

    from . import RichText


TextType = Union["RichText", str]


class InvalidResponse(rich.prompt.InvalidResponse):
    pass


class RichInputMixin:
    """Prevents exc message from printing in the same line on Ctrl + D/C."""

    @classmethod
    def get_input(cls, console: "RichConsole", *args, **kwargs) -> str:
        try:
            return super().get_input(  # type: ignore[misc]
                console, *args, **kwargs
            )
        except (KeyboardInterrupt, EOFError):
            console.print()
            raise


class Prompt(RichInputMixin, rich.prompt.Prompt):
    """Extended version supports custom validations and omission of values."""

    omit_value: str = "n"

    def __init__(
        self,
        *args: Any,
        allow_omission: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.allow_omission: bool = allow_omission

    @classmethod
    def ask(  # pylint: disable=arguments-differ
        cls,
        *args: Any,
        allow_omission: bool = False,
        validator: Optional[
            Callable[[str], Union[str, Tuple[str, str]]]
        ] = None,
        **kwargs: Any,
    ) -> str:
        """Extended to pass validator argument.

        Args:
            allow_omission: Allows omitting prompts
            validator: Validates entered value through prompts
              validator can raise an InvalidResponseError, after which
              it'll reprompt user. It can also return:
              - processed/modified value that will be return to the user.
              - tuple of processed value and a message to show to the user.
              Note that the validator is always required to return a value,
              even if it does not process/modify it.
        """
        default = kwargs.pop("default", None)
        stream = kwargs.pop("stream", None)

        return cls(*args, allow_omission=allow_omission, **kwargs)(
            default=default if default is not None else ...,
            stream=stream,
            validator=validator,
        )

    @classmethod
    def prompt_(
        cls,
        *args: Any,
        allow_omission: bool = False,
        validator: Optional[
            Callable[[str], Union[str, Tuple[str, str]]]
        ] = None,
        **kwargs: Any,
    ) -> Optional[str]:
        """Extends `.ask()` to allow skipping/returning None type."""
        value = cls.ask(
            *args, allow_omission=allow_omission, validator=validator, **kwargs
        )
        if allow_omission and value == cls.omit_value:
            return None
        return value

    def __call__(
        self,
        *args: Any,
        validator: Optional[
            Callable[[str], Union[str, Tuple[str, str]]]
        ] = None,
        **kwargs: Any,
    ) -> str:
        """Supports validating response and show warning message."""

        # We cannot put this in `process_response` as it does not validate
        # `default` values.
        while True:
            value = super().__call__(*args, **kwargs)
            if validator is None or (
                self.allow_omission and value == self.omit_value
            ):
                return value

            try:
                validated = validator(value)
            except InvalidResponse as exc:
                self.on_validate_error(value, exc)
                continue
            else:
                if isinstance(validated, tuple):
                    value, message = validated
                else:
                    value, message = validated, ""
                if message:
                    self.console.print(message)
            return value

    def process_response(self, value: str) -> str:
        """Disallow empty values."""
        ret = super().process_response(value)
        if not ret:
            raise InvalidResponse(
                "[prompt.invalid]Response required. Please try again."
            )
        return ret

    def render_default(self, default):
        from rich.text import Text

        return Text(f"{default!s}", "green")

    def make_prompt(self, default):
        prompt = self.prompt.copy()
        prompt.end = ""

        parts = []
        if (
            default is not ...
            and self.show_default
            and isinstance(default, (str, self.response_type))
        ):
            _default = self.render_default(default)
            parts.append(_default)

        if self.allow_omission:
            from rich.text import Text

            if parts:
                parts.append(", ")
            parts.append(Text(f"{self.omit_value} to omit", style="italic"))

        if parts:
            parts = [" [", *parts, "]"]

        for part in parts:
            prompt.append(part)
        prompt.append(self.prompt_suffix)
        return prompt


class Confirm(RichInputMixin, rich.prompt.Confirm):
    def make_prompt(self, default):
        prompt = self.prompt.copy()
        prompt.end = ""

        prompt.append(" [")
        yes, no = self.choices
        for idx, (val, choice) in enumerate(((True, yes), (False, no))):
            if idx:
                prompt.append("/")
            if val == default:
                prompt.append(choice.upper(), "green")
            else:
                prompt.append(choice)
        prompt.append("]")
        prompt.append(self.prompt_suffix)
        return prompt
