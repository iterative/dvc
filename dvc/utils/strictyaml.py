"""
This module combines schema and yaml parser into one, to provide better error
messages through a single entrypoint `load`.

Used for parsing dvc.yaml, dvc.lock and .dvc files.

Not to be confused with strictyaml, a python library with similar motivations.
"""
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
)

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
    from ruamel.yaml.comments import LineCol
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

        lines: List[object] = []
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


def _normalize_linecol(lc: "LineCol | Tuple[Any, Any]") -> Tuple[int, int]:
    from ruamel.yaml.comments import LineCol

    line, col = None, None

    if isinstance(lc, LineCol):
        line = lc.line
        col = lc.col
    elif isinstance(lc, tuple):
        line = int(lc[0])
        col = int(lc[1])
    else:
        raise TypeError(f"Expected LineCol or tuple, got {lc!r}")

    assert isinstance(line, int)
    assert isinstance(col, int)

    return line + 1, col + 1


def determine_linecol(
    yaml_data: Any, glob_pattern: Sequence[str], key_or_value: bool = False
) -> Tuple[int, int]:
    """
    Return the line and column number for the given location in the data.

    Args:
        yaml_data: The data must be a parsed YAML document represented as a Python
            object, obtained by calling `ruamel.yaml.YAML(typ='rt').load()` on a YAML
            file. The function expects the data to contain information about the
            original YAML document's structure, including the line and column numbers
            where each element appears.
        glob_pattern: The glob pattern to match a key or item in the data.
        key_or_value: A boolean flag that controls whether the result points to the key
            or value of the matching key/value pair. Ignored when matching an item
            within a sequence. Defaults to False.

    Raises:
        ValueError: Raised if more than one leaf matches the glob.
        KeyError: Raised if the location not found.

    Returns:
        A tuple containing the line and column number of the matched location.
    """
    from dpath import get
    from ruamel.yaml.comments import CommentedMap, CommentedSeq

    obj = get(yaml_data, glob_pattern[:-1])
    if isinstance(obj, CommentedMap):
        return _normalize_linecol(
            obj.lc.key(glob_pattern[-1])
            if key_or_value
            else obj.lc.value(glob_pattern[-1])
        )
    if isinstance(obj, CommentedSeq):
        return _normalize_linecol(obj.lc.item(int(glob_pattern[-1])))

    raise TypeError(
        f"Expected commented seq or map, got {type(obj)} at path {glob_pattern!r}"
    )


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

    def _prepare_context(self, data: Mapping) -> List[object]:
        lines: List[object] = []
        for index, error in enumerate(self.exc.errors):
            if index and lines[-1]:
                lines.append("")

            try:
                line, col = determine_linecol(
                    data,
                    error.path,
                    # Handle the case where a validation error indicates that additional
                    # keys are not permitted.
                    key_or_value="extra keys not allowed" in error.error_message,
                )
            except KeyError:
                # Handle the case where a validation error occurs because a required
                # key is missing.
                line, col = determine_linecol(data, error.path[:-1], True)

            parts = [error.error_message]
            if error.path:
                parts.append("in " + " -> ".join(str(p) for p in error.path))
            if line:
                parts.append(f"line {line}")
            if col:
                parts.append(f"column {col}")
            lines.append(_prepare_cause(", ".join(parts)))
            code = _prepare_code_snippets(self.text, line_range=(line - 1, line + 1))
            lines.append(code)
        return lines

    def __pretty_exc__(self, **kwargs: Any) -> None:
        """Prettify exception message."""
        from collections.abc import Mapping

        lines: List[object] = []
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
