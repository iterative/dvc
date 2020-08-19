"""Common utilities for serialize."""
import os
from contextlib import contextmanager

from dvc.exceptions import DvcException
from dvc.utils import relpath


class ParseError(DvcException):
    """Errors while parsing files"""

    def __init__(self, path, message):
        path = relpath(path)
        super().__init__(f"unable to read: '{path}', {message}")


def _load_data(path, parser, tree=None):
    open_fn = tree.open if tree else open
    with open_fn(path, encoding="utf-8") as fd:
        return parser(fd.read(), path)


def _dump_data(path, data, dumper, tree=None):
    open_fn = tree.open if tree else open
    with open_fn(path, "w+", encoding="utf-8") as fd:
        dumper(data, fd)


@contextmanager
def _modify_data(path, parser, dumper, tree=None):
    exists = tree.exists if tree else os.path.exists
    data = _load_data(path, parser=parser, tree=tree) if exists(path) else {}
    yield data
    dumper(path, data, tree=tree)
