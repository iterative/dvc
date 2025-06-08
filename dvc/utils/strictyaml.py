"""
This module combines schema and yaml parser into one, to provide better error
messages through a single entrypoint `load`.

Used for parsing dvc.yaml, dvc.lock and .dvc files.

Not to be confused with strictyaml, a python library with similar motivations.
"""

import re
import typing
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Callable, Optional, TypeVar

from dvc.exceptions import PrettyDvcException
from dvc.ui import ui
from dvc.utils.serialize import (
    EncodingError,
    YAMLFileCorruptedError,
    parse_yaml,
    parse_yaml_for_update,
)

if TYPE_CHECKING:
    from rich.syntax import Syntax
    from ruamel.yaml import StreamMark
    from voluptuous import MultipleInvalid

    from dvc.fs import FileSystem
    from dvc.ui import RichText


_T = TypeVar("_T")
merge_conflict_marker = re.compile("^([<=>]{7}) .*$", re.MULTILINE)


def make_relpath(path: str) -> str:
    import os

    from dvc.utils import relpath

    rel = relpath(path)
    prefix = ""
    if not rel.startswith(".."):
        prefix = "./" if os.name == "posix" else ".\\"
    return prefix + rel


def _prepare_message(message: str) -> "RichText":
    return ui.rich_text(message, style="red")


def _prepare_cause(cause: str) -> "RichText":
    return ui.rich_text(cause, style="bold")


def _prepare_code_snippets(code: str, start_line: int = 1, **kwargs: Any) -> "Syntax":
    from rich.syntax import Syntax

    kwargs.setdefault("start_line", start_line)
    return Syntax(
        code,
        "yaml",
        theme="ansi_dark",
        word_wrap=True,
        line_numbers=True,
        indent_guides=True,
        **kwargs,
    )


class YAMLSyntaxError(PrettyDvcException, YAMLFileCorruptedError):
    def __init__(
        self,
        path: str,
        yaml_text: str,
        exc: Exception,
        rev: Optional[str] = None,
    ) -> None:
        self.path: str = path
        self.yaml_text: str = yaml_text
        self.exc: Exception = exc

        merge_conflicts = merge_conflict_marker.search(self.yaml_text)
        self.hint = " (possible merge conflicts)" if merge_conflicts else ""
        self.rev: Optional[str] = rev
        super().__init__(self.path)

    def __pretty_exc__(self, **kwargs: Any) -> None:  # noqa: C901
        from ruamel.yaml.error import MarkedYAMLError

        exc = self.exc.__cause__

        if not isinstance(exc, MarkedYAMLError):
            raise ValueError("nothing to pretty-print here.")  # noqa: TRY004

        source = self.yaml_text.splitlines()

        def prepare_linecol(mark: "StreamMark") -> str:
            return f"in line {mark.line + 1}, column {mark.column + 1}"

        def prepare_message(
            message: str, mark: Optional["StreamMark"] = None
        ) -> "RichText":
            cause = ", ".join(
                [message.capitalize(), prepare_linecol(mark) if mark else ""]
            )
            return _prepare_cause(cause)

        def prepare_code(mark: "StreamMark") -> "Syntax":
            line = mark.line + 1
            code = "" if line > len(source) else source[line - 1]
            return _prepare_code_snippets(code, line)

        lines: list[object] = []
        if hasattr(exc, "context"):
            if exc.context_mark is not None:
                lines.append(prepare_message(str(exc.context), exc.context_mark))
            if exc.context_mark is not None and (
                exc.problem is None
                or exc.problem_mark is None
                or exc.context_mark.name != exc.problem_mark.name
                or exc.context_mark.line != exc.problem_mark.line
                or exc.context_mark.column != exc.problem_mark.column
            ):
                lines.extend([prepare_code(exc.context_mark), ""])
            if exc.problem is not None:
                lines.append(prepare_message(str(exc.problem), exc.problem_mark))
            if exc.problem_mark is not None:
                lines.append(prepare_code(exc.problem_mark))

        if lines:
            # we should not add a newline after the main message
            # if there are no other outputs
            lines.insert(0, "")

        rel = make_relpath(self.path)
        rev_msg = f" in revision '{self.rev[:7]}'" if self.rev else ""
        msg_fmt = f"'{rel}' is invalid{self.hint}{rev_msg}."
        lines.insert(0, _prepare_message(msg_fmt))
        for line in lines:
            ui.error_write(line, styled=True)


def determine_linecol(
    data, paths, max_steps=5
) -> tuple[Optional[int], Optional[int], int]:
    """Determine linecol from the CommentedMap for the `paths` location.

    CommentedMap from `ruamel.yaml` has `.lc` property from which we can read
    `.line` and `.col`. This is available in the collections type,
    i.e. list and dictionaries.

    But this may fail on non-collection types. For example, if the `paths` is
    ['stages', 'metrics'], metrics being a boolean type does not have `lc`
    prop.
    ```
    stages:
      metrics: true
    ```

    To provide some context to the user, we step up to the
    path ['stages'], which being a collection type, will have `lc` prop
    with which we can find line and col.

    This may end up being not accurate, so we try to show the same amount of
    lines of code for `n` number of steps taken upwards. In a worst case,
    it may be just 1 step (as non-collection item cannot have child items),
    but `schema validator` may provide us arbitrary path. So, this caps the
    number of steps upward to just 5. If it does not find any linecols, it'll
    abort.
    """
    from dpath import get

    step = 1
    line, col = None, None
    while paths and step < max_steps:
        value = get(data, paths, default=None)
        if value is not None:
            with suppress(AttributeError, TypeError):
                line = value.lc.line + 1  # type: ignore[attr-defined]
                col = value.lc.col + 1  # type: ignore[attr-defined]
                break
        step += 1
        *paths, _ = paths

    return line, col, step


class YAMLValidationError(PrettyDvcException):
    def __init__(
        self,
        exc: "MultipleInvalid",
        path: Optional[str] = None,
        text: Optional[str] = None,
        rev: Optional[str] = None,
    ) -> None:
        self.text = text or ""
        self.exc = exc

        rel = make_relpath(path) if path else ""
        self.path = path or ""

        message = f"'{rel}' validation failed"
        message += f" in revision '{rev[:7]}'" if rev else ""
        if len(self.exc.errors) > 1:
            message += f": {len(self.exc.errors)} errors"
        super().__init__(f"{message}")

    def _prepare_context(self, data: typing.Mapping) -> list[object]:
        lines: list[object] = []
        for index, error in enumerate(self.exc.errors):
            if index and lines[-1]:
                lines.append("")
            line, col, step = determine_linecol(data, error.path)
            parts = [error.error_message]
            if error.path:
                parts.append("in " + " -> ".join(str(p) for p in error.path))
            if line:
                parts.append(f"line {line}")
            if col:
                parts.append(f"column {col}")
            lines.append(_prepare_cause(", ".join(parts)))

            if line:
                # we show one line above the error
                # we try to show few more lines if we could not
                # reliably figure out where the error was
                lr = (line - 1, line + step - 1)
                code = _prepare_code_snippets(self.text, line_range=lr)
                lines.append(code)
        return lines

    def __pretty_exc__(self, **kwargs: Any) -> None:
        """Prettify exception message."""
        from collections.abc import Mapping

        lines: list[object] = []
        data = parse_yaml_for_update(self.text, self.path)
        if isinstance(data, Mapping):
            lines.extend(self._prepare_context(data))

        cause = ""
        if lines:
            # we should not add a newline after the main message
            # if there are no other outputs
            lines.insert(0, "")
        else:
            # if we don't have any context to show, we'll fallback to what we
            # got from voluptuous and print them in the same line.
            cause = f": {self.exc}"

        lines.insert(0, _prepare_message(f"{self}{cause}."))
        for line in lines:
            ui.error_write(line, styled=True)


def validate(
    data: _T,
    schema: Callable[[_T], _T],
    text: Optional[str] = None,
    path: Optional[str] = None,
    rev: Optional[str] = None,
) -> _T:
    from voluptuous import MultipleInvalid

    try:
        return schema(data)
    except MultipleInvalid as exc:
        raise YAMLValidationError(exc, path, text, rev=rev) from exc


def load(
    path: str,
    schema: Optional[Callable[[_T], _T]] = None,
    fs: Optional["FileSystem"] = None,
    encoding: str = "utf-8",
    round_trip: bool = False,
) -> Any:
    open_fn = fs.open if fs else open
    rev = getattr(fs, "rev", None)

    try:
        with open_fn(path, encoding=encoding) as fd:  # type: ignore[operator]
            text = fd.read()
        data = parse_yaml(text, path, typ="rt" if round_trip else "safe")
    except UnicodeDecodeError as exc:
        raise EncodingError(path, encoding) from exc
    except YAMLFileCorruptedError as exc:
        cause = exc.__cause__
        raise YAMLSyntaxError(path, text, exc, rev=rev) from cause

    if schema:
        # not returning validated data, as it may remove
        # details from CommentedMap that we get from roundtrip parser
        validate(data, schema, text=text, path=path, rev=rev)
    return data, text
