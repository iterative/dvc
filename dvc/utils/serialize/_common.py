"""Common utilities for serialize."""
import os
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Optional,
    Protocol,
    TextIO,
    Union,
)

from funcy import reraise

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.types import StrPath


class DumperFn(Protocol):
    def __call__(
        self, path: "StrPath", data: Any, fs: Optional["FileSystem"] = None
    ) -> Any:
        ...


class DumpersFn(Protocol):
    def __call__(self, data: Any, stream: TextIO) -> Any:
        ...


class ModifierFn(Protocol):
    def __call__(
        self, path: "StrPath", fs: Optional["FileSystem"] = None
    ) -> ContextManager[Dict]:
        ...


class LoaderFn(Protocol):
    def __call__(self, path: "StrPath", fs: Optional["FileSystem"] = None) -> Any:
        ...


ReadType = Union[bytes, None, str]
ParserFn = Callable[[ReadType, "StrPath"], dict]


class ParseError(DvcException):
    """Errors while parsing files"""

    def __init__(self, path: "StrPath", message: str):
        from dvc.utils import relpath

        path = relpath(path)
        self.path = path
        super().__init__(f"unable to read: '{path}', {message}")


class EncodingError(ParseError):
    """We could not read a file with the given encoding"""

    def __init__(self, path: "StrPath", encoding: str):
        self.encoding = encoding
        super().__init__(path, f"is not valid {encoding}")


def _load_data(
    path: "StrPath", parser: ParserFn, fs: Optional["FileSystem"] = None, **kwargs
):
    open_fn = fs.open if fs else open
    encoding = "utf-8"
    with open_fn(path, encoding=encoding, **kwargs) as fd:  # type: ignore[operator]
        with reraise(UnicodeDecodeError, EncodingError(path, encoding)):
            return parser(fd.read(), path)


def _dump_data(
    path,
    data: Any,
    dumper: DumpersFn,
    fs: Optional["FileSystem"] = None,
    **dumper_args,
):
    open_fn = fs.open if fs else open
    with open_fn(path, "w+", encoding="utf-8") as fd:  # type: ignore[operator]
        dumper(data, fd, **dumper_args)


@contextmanager
def _modify_data(
    path: "StrPath",
    parser: ParserFn,
    dumper: DumpersFn,
    fs: Optional["FileSystem"] = None,
):
    file_exists = fs.exists(os.fspath(path)) if fs else os.path.exists(path)
    data = _load_data(path, parser=parser, fs=fs) if file_exists else {}
    yield data
    _dump_data(path, data, dumper=dumper, fs=fs)
