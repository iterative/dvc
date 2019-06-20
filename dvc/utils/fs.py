from __future__ import unicode_literals

import os
import errno
import nanotime
import logging

from dvc.exceptions import DvcException
from dvc.system import System
from dvc.utils import dvc_walk, dict_md5
from dvc.utils.compat import str


logger = logging.getLogger(__name__)


def get_inode(path):
    inode = System.inode(path)
    logger.debug("Path {} inode {}".format(path, inode))
    return inode


def get_mtime_signature_and_size(path, ignore_file_handler=None):
    base_stat = os.stat(path)
    size = base_stat.st_size

    if os.path.isdir(path):
        files_mtimes = {}
        for root, dirs, files in dvc_walk(
            str(path), ignore_file_handler=ignore_file_handler
        ):
            for dir in dirs:
                entry = os.path.join(root, dir)
                size += os.path.getsize(entry)

            for file in files:
                entry = os.path.join(root, file)
                try:
                    stat = os.stat(entry)
                except OSError as exc:
                    # NOTE: broken symlink case.
                    if exc.errno != errno.ENOENT:
                        raise
                    continue
                size += stat.st_size
                files_mtimes[entry] = stat.st_mtime

        # Why mtime for dir is actually dict_md5 from {file_path:mtime} pairs?
        # In case of updating .dvcignore-d file in dir, mtime for directory
        # would be updated. We don't want to detect that, yet we have to detect
        # operations that results in dvc tracked directory mtime update and not
        # file mtime updates (e.g. moving tracked file), hence we need to
        # combine update mtimes with file_paths
        mtime_signature = dict_md5(files_mtimes)
    else:
        mtime_signature = base_stat.st_mtime
        mtime_signature = int(nanotime.timestamp(mtime_signature))

    # State of files handled by dvc is stored in db as TEXT.
    # We cast results to string for later comparisons with stored values.
    return str(mtime_signature), str(size)


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


def get_parent_dirs_up_to(wdir, root_dir):

    assert os.path.isabs(wdir)
    assert os.path.isabs(root_dir)

    wdir = os.path.normpath(wdir)
    root_dir = os.path.normpath(root_dir)
    if root_dir not in wdir:
        return []

    dirs = []
    dirs.append(wdir)
    while wdir != root_dir:
        wdir = os.path.dirname(wdir)
        dirs.append(wdir)

    return dirs
