import errno
import logging
import os
import shutil
import stat
import sys
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING

from dvc.exceptions import DvcException
from dvc.system import System
from dvc.utils import dict_md5

if TYPE_CHECKING:
    from dvc.types import StrPath

logger = logging.getLogger(__name__)

LOCAL_CHUNK_SIZE = 2**20  # 1 MB

umask = os.umask(0)
os.umask(umask)


def get_inode(path):
    inode = System.inode(path)
    logger.trace("Path '%s' inode '%d'", path, inode)
    return inode


def get_mtime_and_size(path, fs, dvcignore=None):
    import nanotime

    if fs.isdir(path):
        size = 0
        files_mtimes = {}
        if dvcignore:
            walk_iterator = dvcignore.find(fs, path)
        else:
            walk_iterator = fs.find(path)
        for file_path in walk_iterator:
            try:
                stats = fs.info(file_path)
            except OSError as exc:
                # NOTE: broken symlink case.
                if exc.errno != errno.ENOENT:
                    raise
                continue
            size += stats["size"]
            files_mtimes[file_path] = stats["mtime"]

        # We track file changes and moves, which cannot be detected with simply
        # max(mtime(f) for f in non_ignored_files)
        mtime = dict_md5(files_mtimes)
    else:
        base_stat = fs.info(path)
        size = base_stat["size"]
        mtime = base_stat["mtime"]
        mtime = int(nanotime.timestamp(mtime))

    return str(mtime), size


class BasePathNotInCheckedPathException(DvcException):
    def __init__(self, path, base_path):
        msg = "Path: {} does not overlap with base path: {}".format(
            path, base_path
        )
        super().__init__(msg)


def contains_symlink_up_to(path: "StrPath", base_path: "StrPath"):
    base_path = os.path.normcase(os.fspath(base_path))
    path = os.path.normcase(os.fspath(path))

    if base_path not in path:
        raise BasePathNotInCheckedPathException(path, base_path)

    if path == base_path:
        return False
    if System.is_symlink(path):
        return True
    if os.path.dirname(path) == path:
        return False
    return contains_symlink_up_to(os.path.dirname(path), base_path)


def move(src, dst):
    """Atomically move src to dst and chmod it with mode.

    Moving is performed in two stages to make the whole operation atomic in
    case src and dst are on different filesystems and actual physical copying
    of data is happening.
    """
    from shortuuid import uuid

    dst = os.path.abspath(dst)
    tmp = f"{dst}.{uuid()}"

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


def path_isin(child: "StrPath", parent: "StrPath") -> bool:
    """Check if given `child` path is inside `parent`."""

    def normalize_path(path) -> str:
        return os.path.normcase(os.path.normpath(path))

    parent = os.path.join(normalize_path(parent), "")
    child = normalize_path(child)
    return child != parent and child.startswith(parent)


def makedirs(path, exist_ok=False, mode=None):
    if mode is None:
        os.makedirs(path, exist_ok=exist_ok)
        return

    # Modified version of os.makedirs() with support for extended mode
    # (e.g. S_ISGID)
    head, tail = os.path.split(path)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not os.path.exists(head):
        try:
            makedirs(head, exist_ok=exist_ok, mode=mode)
        except FileExistsError:
            # Defeats race condition when another thread created the path
            pass
        cdir = os.curdir
        if isinstance(tail, bytes):
            cdir = bytes(os.curdir, "ASCII")
        if tail == cdir:  # xxx/newdir/. exists if xxx/newdir exists
            return
    try:
        os.mkdir(path, mode)
    except OSError:
        # Cannot rely on checking for EEXIST, since the operating system
        # could give priority to other errors like EACCES or EROFS
        if not exist_ok or not os.path.isdir(path):
            raise

    try:
        os.chmod(path, mode)
    except OSError:
        logger.trace("failed to chmod '%o' '%s'", mode, path, exc_info=True)


def copyfile(src, dest, callback=None, no_progress_bar=False, name=None):
    """Copy file with progress bar"""
    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    if callback:
        callback.set_size(total)

    try:
        System.reflink(src, dest)
    except OSError:
        from dvc.fs._callback import tdqm_or_callback_wrapped

        with open(src, "rb") as fsrc, open(dest, "wb+") as fdest:
            with tdqm_or_callback_wrapped(
                fdest,
                "write",
                total,
                callback=callback,
                disable=no_progress_bar,
                desc=name,
            ) as wrapped:
                while True:
                    buf = fsrc.read(LOCAL_CHUNK_SIZE)
                    if not buf:
                        break
                    wrapped.write(buf)

    if callback:
        callback.absolute_update(total)


def copy_fobj_to_file(fsrc, dest):
    """Copy contents of open file object to destination path."""
    with open(dest, "wb+") as fdest:
        shutil.copyfileobj(fsrc, fdest)


def walk_files(directory):
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)


@contextmanager
def as_atomic(fs, to_info):
    from dvc.utils import tmp_fname

    tmp_info = fs.path.join(fs.path.parent(to_info), tmp_fname())
    try:
        yield tmp_info
    except BaseException:
        # Handle stuff like KeyboardInterrupt
        # as well as other errors that might
        # arise during file transfer.
        with suppress(FileNotFoundError):
            fs.remove(tmp_info)
        raise
    else:
        fs.move(tmp_info, to_info)
