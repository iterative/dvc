import errno
import logging
import os
import shutil
import stat
import sys

import nanotime
from shortuuid import uuid

from dvc.exceptions import DvcException, FileOwnershipError
from dvc.system import System
from dvc.utils import dict_md5

logger = logging.getLogger(__name__)

LOCAL_CHUNK_SIZE = 2 ** 20  # 1 MB


def fs_copy(src, dst):
    if os.path.isdir(src):
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)


def get_inode(path):
    inode = System.inode(path)
    logger.debug("Path '%s' inode '%d'", path, inode)
    return inode


def get_mtime_and_size(path, tree):

    if tree.isdir(path):
        size = 0
        files_mtimes = {}
        for file_path in tree.walk_files(path):
            try:
                stats = tree.stat(file_path)
            except OSError as exc:
                # NOTE: broken symlink case.
                if exc.errno != errno.ENOENT:
                    raise
                continue
            size += stats.st_size
            files_mtimes[os.fspath(file_path)] = stats.st_mtime

        # We track file changes and moves, which cannot be detected with simply
        # max(mtime(f) for f in non_ignored_files)
        mtime = dict_md5(files_mtimes)
    else:
        base_stat = tree.stat(path)
        size = base_stat.st_size
        mtime = base_stat.st_mtime
        mtime = int(nanotime.timestamp(mtime))

    # State of files handled by dvc is stored in db as TEXT.
    # We cast results to string for later comparisons with stored values.
    return str(mtime), str(size)


class BasePathNotInCheckedPathException(DvcException):
    def __init__(self, path, base_path):
        msg = "Path: {} does not overlap with base path: {}".format(
            path, base_path
        )
        super().__init__(msg)


def contains_symlink_up_to(path, base_path):
    base_path = os.fspath(base_path)
    path = os.fspath(path)

    if base_path not in path:
        raise BasePathNotInCheckedPathException(path, base_path)

    if path == base_path:
        return False
    if System.is_symlink(path):
        return True
    if os.path.dirname(path) == path:
        return False
    return contains_symlink_up_to(os.path.dirname(path), base_path)


def move(src, dst, mode=None):
    """Atomically move src to dst and chmod it with mode.

    Moving is performed in two stages to make the whole operation atomic in
    case src and dst are on different filesystems and actual physical copying
    of data is happening.
    """

    dst = os.path.abspath(dst)
    tmp = f"{dst}.{uuid()}"

    try:
        if mode is not None:
            os.chmod(src, mode)
    except OSError as e:
        if e.errno not in [errno.EACCES, errno.EPERM]:
            raise
        else:
            raise FileOwnershipError(src)

    if os.path.islink(src):
        shutil.copy(src, tmp)
        _unlink(src, _chmod)
    else:
        shutil.move(src, tmp)

    shutil.move(tmp, dst)


def _chmod(func, p, excinfo):  # pylint: disable=unused-argument
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


def path_isin(child, parent):
    """Check if given `child` path is inside `parent`."""

    def normalize_path(path):
        return os.path.normpath(path)

    parent = os.path.join(normalize_path(parent), "")
    child = normalize_path(child)
    return child != parent and child.startswith(parent)


def makedirs(path, exist_ok=False, mode=None):
    if mode is None:
        os.makedirs(path, exist_ok=exist_ok)
        return

    # utilize umask to set proper permissions since Python 3.7 the `mode`
    # `makedirs` argument no longer affects the file permission bits of
    # newly-created intermediate-level directories.
    umask = os.umask(0o777 - mode)
    try:
        os.makedirs(path, exist_ok=exist_ok)
    finally:
        os.umask(umask)


def copyfile(src, dest, no_progress_bar=False, name=None):
    """Copy file with progress bar"""
    from dvc.progress import Tqdm

    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    try:
        System.reflink(src, dest)
    except DvcException:
        with open(src, "rb") as fsrc, open(dest, "wb+") as fdest:
            with Tqdm.wrapattr(
                fdest,
                "write",
                desc=name,
                disable=no_progress_bar,
                total=total,
                bytes=True,
            ) as fdest_wrapped:
                while True:
                    buf = fsrc.read(LOCAL_CHUNK_SIZE)
                    if not buf:
                        break
                    fdest_wrapped.write(buf)


def copy_fobj_to_file(fsrc, dest):
    """Copy contents of open file object to destination path."""
    with open(dest, "wb+") as fdest:
        shutil.copyfileobj(fsrc, fdest)


def walk_files(directory):
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)
