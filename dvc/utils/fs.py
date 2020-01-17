import errno
import logging
import os
import shutil
import stat

import nanotime
from shortuuid import uuid

from dvc.exceptions import DvcException
from dvc.scm.tree import is_working_tree
from dvc.system import System
from dvc.utils import dict_md5
from dvc.utils import fspath
from dvc.utils import fspath_py35


logger = logging.getLogger(__name__)

LOCAL_CHUNK_SIZE = 2 ** 20  # 1 MB


def fs_copy(src, dst):
    if os.path.isdir(src):
        shutil.copytree(src, dst)
    else:
        shutil.copy2(src, dst)


def get_inode(path):
    inode = System.inode(path)
    logger.debug("Path {} inode {}", path, inode)
    return inode


def get_mtime_and_size(path, tree):

    if os.path.isdir(fspath_py35(path)):
        assert is_working_tree(tree)

        size = 0
        files_mtimes = {}
        for file_path in tree.walk_files(path):
            try:
                stat = os.stat(file_path)
            except OSError as exc:
                # NOTE: broken symlink case.
                if exc.errno != errno.ENOENT:
                    raise
                continue
            size += stat.st_size
            files_mtimes[file_path] = stat.st_mtime

        # We track file changes and moves, which cannot be detected with simply
        # max(mtime(f) for f in non_ignored_files)
        mtime = dict_md5(files_mtimes)
    else:
        base_stat = os.stat(fspath_py35(path))
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
    base_path = fspath(base_path)
    path = fspath(path)

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

    src = fspath_py35(src)
    dst = fspath_py35(dst)

    dst = os.path.abspath(dst)
    tmp = "{}.{}".format(dst, uuid())

    if os.path.islink(src):
        shutil.copy(os.readlink(src), tmp)
        os.unlink(src)
    else:
        shutil.move(src, tmp)

    if mode is not None:
        os.chmod(tmp, mode)

    shutil.move(tmp, dst)


def _chmod(func, p, excinfo):
    perm = os.lstat(p).st_mode
    perm |= stat.S_IWRITE

    try:
        os.chmod(p, perm)
    except OSError as exc:
        # broken symlink or file is not owned by us
        if exc.errno not in [errno.ENOENT, errno.EPERM]:
            raise

    func(p)


def remove(path):
    logger.debug("Removing '{}'", path)

    path = fspath_py35(path)
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, onerror=_chmod)
        else:
            _chmod(os.unlink, path, None)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def path_isin(child, parent):
    """Check if given `child` path is inside `parent`."""

    def normalize_path(path):
        return os.path.normpath(fspath_py35(path))

    parent = os.path.join(normalize_path(parent), "")
    child = normalize_path(child)
    return child != parent and child.startswith(parent)


def makedirs(path, exist_ok=False, mode=None):
    path = fspath_py35(path)

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
    from dvc.exceptions import DvcException
    from dvc.progress import Tqdm
    from dvc.system import System

    src = fspath_py35(src)
    dest = fspath_py35(dest)

    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    try:
        System.reflink(src, dest)
    except DvcException:
        with Tqdm(
            desc=name, disable=no_progress_bar, total=total, bytes=True
        ) as pbar:
            with open(src, "rb") as fsrc, open(dest, "wb+") as fdest:
                while True:
                    buf = fsrc.read(LOCAL_CHUNK_SIZE)
                    if not buf:
                        break
                    fdest.write(buf)
                    pbar.update(len(buf))


def walk_files(directory):
    for root, _, files in os.walk(fspath(directory)):
        for f in files:
            yield os.path.join(root, f)
