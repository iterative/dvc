"""Common utilities for serialize."""

from dvc.exceptions import DvcException
from dvc.utils import relpath


class ParseError(DvcException):
    """Errors while parsing files"""

    def __init__(self, path, message):
        path = relpath(path)
        super().__init__(f"unable to read: '{path}', {message}")
