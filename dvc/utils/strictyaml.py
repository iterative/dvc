"""
This module combines schema and yaml parser into one, to provide better error
messages through a single entrypoint `load`.

Used for parsing dvc.yaml, dvc.lock and .dvc files.

Not to be confused with strictyaml, a python library with similar motivations.
"""
from typing import TYPE_CHECKING, Any, Callable, List, TypeVar

from dvc.exceptions import DvcException, PrettyDvcException
from dvc.utils.serialize import (
    EncodingError,
    YAMLFileCorruptedError,
    parse_yaml,
)

if TYPE_CHECKING:
    from rich.syntax import Syntax
    from rich.text import Text
    from ruamel.yaml import StreamMark

    from dvc.fs.base import BaseFileSystem


_T = TypeVar("_T")


def _prepare_cause(cause: str) -> "Text":
    from rich.text import Text

    return Text(cause, style="bold")


def _prepare_code_snippets(code: str, start_line: int = 1) -> "Syntax":
    from rich.syntax import Syntax

    return Syntax(
        code,
        "yaml",
        start_line=start_line,
        theme="ansi_dark",
        word_wrap=True,
        line_numbers=True,
        indent_guides=True,
    )


class YAMLSyntaxError(PrettyDvcException, YAMLFileCorruptedError):
    def __init__(self, path: str, yaml_text: str, exc: Exception) -> None:
        self.path: str = path
        self.yaml_text: str = yaml_text
        self.exc: Exception = exc
        super().__init__(self.path)

    def __pretty_exc__(self, **kwargs: Any) -> None:
        from ruamel.yaml.error import MarkedYAMLError

        from dvc.ui import ui
        from dvc.utils import relpath

        exc = self.exc.__cause__

        if not isinstance(exc, MarkedYAMLError):
            raise ValueError("nothing to pretty-print here. :)")

        source = self.yaml_text.splitlines()

        def prepare_linecol(mark: "StreamMark") -> str:
            return f"in line {mark.line + 1}, column {mark.column + 1}"

        def prepare_message(message: str, mark: "StreamMark" = None) -> "Text":
            cause = ", ".join(
                [message.capitalize(), prepare_linecol(mark) if mark else ""]
            )
            return _prepare_cause(cause)

        def prepare_code(mark: "StreamMark") -> "Syntax":
            line = mark.line + 1
            code = "" if line > len(source) else source[line - 1]
            return _prepare_code_snippets(code, line)

        lines: List[object] = []
        if hasattr(exc, "context"):
            if exc.context_mark is not None:
                lines.append(
                    prepare_message(str(exc.context), exc.context_mark)
                )
            if exc.context_mark is not None and (
                exc.problem is None
                or exc.problem_mark is None
                or exc.context_mark.name != exc.problem_mark.name
                or exc.context_mark.line != exc.problem_mark.line
                or exc.context_mark.column != exc.problem_mark.column
            ):
                lines.extend([prepare_code(exc.context_mark), ""])
            if exc.problem is not None:
                lines.append(
                    prepare_message(str(exc.problem), exc.problem_mark)
                )
            if exc.problem_mark is not None:
                lines.append(prepare_code(exc.problem_mark))

        if lines and lines[-1]:
            lines.insert(0, "")
        lines.insert(0, f"[red]'{relpath(self.path)}' structure is corrupted.")
        for message in lines:
            ui.error_write(message, styled=True)


class YAMLValidationError(DvcException):
    def __init__(self, exc):
        super().__init__(str(exc))


def validate(data: _T, schema: Callable[[_T], _T], _text: str = None) -> _T:
    from voluptuous import MultipleInvalid

    try:
        return schema(data)
    except MultipleInvalid as exc:
        raise YAMLValidationError(str(exc))


def load(
    path: str,
    schema: Callable[[_T], _T] = None,
    fs: "BaseFileSystem" = None,
    encoding: str = "utf-8",
    round_trip: bool = False,
) -> Any:
    open_fn = fs.open if fs else open
    try:
        with open_fn(path, encoding=encoding) as fd:  # type: ignore
            text = fd.read()
        data = parse_yaml(text, path, typ="rt" if round_trip else "safe")
    except UnicodeDecodeError as exc:
        raise EncodingError(path, encoding) from exc
    except YAMLFileCorruptedError as exc:
        cause = exc.__cause__
        raise YAMLSyntaxError(path, text, exc) from cause

    if schema:
        # not returning validated data, as it may remove
        # details from CommentedMap that we get from roundtrip parser
        validate(data, schema, text)
    return data, text
