import errno
import logging
import os
import shutil
import stat
import sys
from typing import TYPE_CHECKING

from dvc.exceptions import DvcException

if TYPE_CHECKING:
    from dvc.types import StrPath

logger = logging.getLogger(__name__)


class BasePathNotInCheckedPathException(DvcException):
    def __init__(self, path, base_path):
        msg = f"Path: {path} does not overlap with base path: {base_path}"
        super().__init__(msg)


def contains_symlink_up_to(path: "StrPath", base_path: "StrPath"):
    from dvc.fs import system

    base_path = os.path.normcase(os.fspath(base_path))
    path = os.path.normcase(os.fspath(path))

    if base_path not in path:
        raise BasePathNotInCheckedPathException(path, base_path)

    if path == base_path:
        return False
    if system.is_symlink(path):
        return True
    if os.path.dirname(path) == path:
        return False
    return contains_symlink_up_to(os.path.dirname(path), base_path)


def _chmod(func, p, excinfo):  # noqa: ARG001, pylint: disable=unused-argument
    perm = os.lstat(p).st_mode
    perm |= stat.S_IWRITE

    try:
        os.chmod(p, perm)
    except OSError as exc:
        # broken symlink or file is not owned by us
        if exc.errno not in [errno.ENOENT, errno.EPERM]:
            raise

    func(p)


def _unlink(path, onerror):
    try:
        os.unlink(path)
    except OSError:
        onerror(os.unlink, path, sys.exc_info())


def remove(path):
    logger.debug("Removing '%s'", path)

    try:
        if os.path.isdir(path):
            shutil.rmtree(path, onerror=_chmod)
        else:
            _unlink(path, _chmod)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def path_isin(child: "StrPath", parent: "StrPath") -> bool:
    """Check if given `child` path is inside `parent`."""

    def normalize_path(path) -> str:
        return os.path.normcase(os.path.normpath(path))

    parent = os.path.join(normalize_path(parent), "")
    child = normalize_path(child)
    return child != parent and child.startswith(parent)
