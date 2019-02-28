from __future__ import unicode_literals

import nanotime
import os

import dvc.logger as logger
from dvc.exceptions import DvcException
from dvc.system import System
from dvc.utils.compat import str


def get_inode(path):
    inode = System.inode(path)
    logger.debug("Path {} inode {}".format(path, inode))
    return inode


def get_mtime_and_size(path):
    size = os.path.getsize(path)
    mtime = os.path.getmtime(path)

    if os.path.isdir(path):
        for root, dirs, files in os.walk(str(path)):
            for name in dirs + files:
                entry = os.path.join(root, name)
                stat = os.stat(entry)
                size += stat.st_size
                entry_mtime = stat.st_mtime
                if entry_mtime > mtime:
                    mtime = entry_mtime

    # State of files handled by dvc is stored in db as TEXT.
    # We cast results to string for later comparisons with stored values.
    return str(int(nanotime.timestamp(mtime))), str(size)


class BasePathNotInCheckedPathException(DvcException):
    def __init__(self, path, base_path):
        msg = "Path: {} does not overlap with base path: {}".format(
            path, base_path
        )
        super(DvcException, self).__init__(msg)


def contains_symlink_up_to(path, base_path):
    if base_path not in path:
        raise BasePathNotInCheckedPathException(path, base_path)

    if path == base_path:
        return False
    if System.is_symlink(path):
        return True
    if os.path.dirname(path) == path:
        return False
    return contains_symlink_up_to(os.path.dirname(path), base_path)
