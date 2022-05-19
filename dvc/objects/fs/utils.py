import errno
import logging
import os
import shutil
import stat
import sys
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Iterator

from . import system

if TYPE_CHECKING:
    from .base import AnyFSPath, FileSystem
    from .callbacks import Callback


logger = logging.getLogger(__name__)


LOCAL_CHUNK_SIZE = 2**20  # 1 MB


def is_exec(mode: int) -> bool:
    return bool(mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH))


def relpath(path: "AnyFSPath", start: "AnyFSPath" = os.curdir) -> "AnyFSPath":
    path = os.fspath(path)
    start = os.path.abspath(os.fspath(start))

    # Windows path on different drive than curdir doesn't have relpath
    if os.name == "nt":
        # Since python 3.8 os.realpath resolves network shares to their UNC
        # path. So, to be certain that relative paths correctly captured,
        # we need to resolve to UNC path first. We resolve only the drive
        # name so that we don't follow any 'real' symlinks on the path
        def resolve_network_drive_windows(path_to_resolve):
            drive, tail = os.path.splitdrive(path_to_resolve)
            return os.path.join(os.path.realpath(drive), tail)

        path = resolve_network_drive_windows(os.path.abspath(path))
        start = resolve_network_drive_windows(start)
        if not os.path.commonprefix([start, path]):
            return path
    return os.path.relpath(path, start)


def move(src: "AnyFSPath", dst: "AnyFSPath") -> None:
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


def _unlink(path: "AnyFSPath", onerror):
    try:
        os.unlink(path)
    except OSError:
        onerror(os.unlink, path, sys.exc_info())


def remove(path: "AnyFSPath") -> None:
    logger.debug("Removing '%s'", path)

    try:
        if os.path.isdir(path):
            shutil.rmtree(path, onerror=_chmod)
        else:
            _unlink(path, _chmod)
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise


def makedirs(path, exist_ok: bool = False, mode: int = None) -> None:
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
            cdir = bytes(os.curdir, "ASCII")  # type: ignore[assignment]
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
        logger.trace(  # type: ignore[attr-defined]
            "failed to chmod '%o' '%s'", mode, path, exc_info=True
        )


def copyfile(
    src: "AnyFSPath",
    dest: "AnyFSPath",
    callback: "Callback" = None,
    no_progress_bar: bool = False,
    name: str = None,
) -> None:
    """Copy file with progress bar"""
    name = name if name else os.path.basename(dest)
    total = os.stat(src).st_size

    if os.path.isdir(dest):
        dest = os.path.join(dest, os.path.basename(src))

    if callback:
        callback.set_size(total)

    try:
        system.reflink(src, dest)
    except OSError:
        from .callbacks import Callback

        with open(src, "rb") as fsrc, open(dest, "wb+") as fdest:
            with Callback.as_tqdm_callback(
                callback,
                size=total,
                bytes=True,
                disable=no_progress_bar,
                desc=name,
            ) as cb:
                wrapped = cb.wrap_attr(fdest, "write")
                while True:
                    buf = fsrc.read(LOCAL_CHUNK_SIZE)
                    if not buf:
                        break
                    wrapped.write(buf)

    if callback:
        callback.absolute_update(total)


def walk_files(directory: "AnyFSPath") -> Iterator["AnyFSPath"]:
    for root, _, files in os.walk(directory):
        for f in files:
            yield os.path.join(root, f)


def tmp_fname(fname: "AnyFSPath" = "") -> "AnyFSPath":
    """Temporary name for a partial download"""
    from shortuuid import uuid

    return os.fspath(fname) + "." + uuid() + ".tmp"


@contextmanager
def as_atomic(
    fs: "FileSystem", to_info: "AnyFSPath", create_parents: bool = False
) -> Iterator["AnyFSPath"]:
    parent = fs.path.parent(to_info)
    if create_parents:
        fs.makedirs(parent, exist_ok=True)

    tmp_info = fs.path.join(parent, tmp_fname())
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


# https://github.com/aws/aws-cli/blob/5aa599949f60b6af554fd5714d7161aa272716f7/awscli/customizations/s3/utils.py
MULTIPLIERS = {
    "kb": 1024,
    "mb": 1024**2,
    "gb": 1024**3,
    "tb": 1024**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}


def human_readable_to_bytes(value: str) -> int:
    value = value.lower()
    suffix = ""
    if value.endswith(tuple(MULTIPLIERS.keys())):
        size = 2
        size += value[-2] == "i"  # KiB, MiB etc
        value, suffix = value[:-size], value[-size:]

    multiplier = MULTIPLIERS.get(suffix, 1)
    return int(value) * multiplier


def flatten(d):
    import flatten_dict

    return flatten_dict.flatten(d, reducer="dot")


def unflatten(d):
    import flatten_dict

    return flatten_dict.unflatten(d, splitter="dot")
