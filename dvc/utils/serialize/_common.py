"""Common utilities for serialize."""
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, ContextManager, Dict, Union

from funcy import reraise
from typing_extensions import Protocol

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc.types import AnyPath


class DumperFn(Protocol):
    def __call__(
        self, path: "AnyPath", data: Any, fs: "FileSystem" = None
    ) -> Any:
        ...


class ModifierFn(Protocol):
    def __call__(
        self, path: "AnyPath", fs: "FileSystem" = None
    ) -> ContextManager[Dict]:
        ...


class LoaderFn(Protocol):
    def __call__(self, path: "AnyPath", fs: "FileSystem" = None) -> Any:
        ...


ReadType = Union[bytes, None, str]
ParserFn = Callable[[ReadType, "AnyPath"], dict]


class ParseError(DvcException):
    """Errors while parsing files"""

    def __init__(self, path: "AnyPath", message: str):
        from dvc.utils import relpath

        path = relpath(path)
        self.path = path
        super().__init__(f"unable to read: '{path}', {message}")


class EncodingError(ParseError):
    """We could not read a file with the given encoding"""

    def __init__(self, path: "AnyPath", encoding: str):
        self.encoding = encoding
        super().__init__(path, f"is not valid {encoding}")


def _load_data(path: "AnyPath", parser: ParserFn, fs: "FileSystem" = None):
    open_fn = fs.open if fs else open
    encoding = "utf-8"
    with open_fn(path, encoding=encoding) as fd:  # type: ignore
        with reraise(UnicodeDecodeError, EncodingError(path, encoding)):
            return parser(fd.read(), path)


def _dump_data(
    path,
    data: Any,
    dumper: DumperFn,
    fs: "FileSystem" = None,
    **dumper_args,
):
    open_fn = fs.open if fs else open
    with open_fn(path, "w+", encoding="utf-8") as fd:  # type: ignore
        dumper(data, fd, **dumper_args)


@contextmanager
def _modify_data(
    path: "AnyPath",
    parser: ParserFn,
    dumper: DumperFn,
    fs: "FileSystem" = None,
):
    exists_fn = fs.exists if fs else os.path.exists
    file_exists = exists_fn(path)  # type: ignore
    data = _load_data(path, parser=parser, fs=fs) if file_exists else {}
    yield data
    dumper(path, data, fs=fs)
