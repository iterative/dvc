import errno
import logging
import os
from typing import TYPE_CHECKING

from .base import RemoteActionNotImplemented
from .local import LocalFileSystem

if TYPE_CHECKING:
    from dvc.types import AnyPath, DvcPath

    from .base import BaseFileSystem

logger = logging.getLogger(__name__)


def _link(
    from_fs: "BaseFileSystem",
    from_info: "AnyPath",
    to_fs: "BaseFileSystem",
    to_info: "DvcPath",
    hardlink: bool = False,
) -> None:
    if not isinstance(from_fs, type(to_fs)):
        raise OSError(errno.EXDEV, "can't link across filesystems")

    links = ["reflink"] + ["hardlink"] if hardlink else []
    while links:
        link = links.pop(0)
        try:
            func = getattr(to_fs, link)
        except AttributeError:
            continue

        try:
            return func(from_info, to_info)
        except RemoteActionNotImplemented:
            continue
        except OSError as exc:
            if exc.errno not in [errno.EXDEV, errno.ENOTSUP]:
                raise

    raise OSError(errno.ENOTSUP, "reflink and hardlink are not supported")


def transfer(
    from_fs: "BaseFileSystem",
    from_info: "AnyPath",
    to_fs: "BaseFileSystem",
    to_info: "DvcPath",
    hardlink: bool = False,
) -> None:
    try:
        try:
            return _link(from_fs, from_info, to_fs, to_info, hardlink=hardlink)
        except OSError as exc:
            if exc.errno not in [errno.EXDEV, errno.ENOTSUP]:
                raise

        if isinstance(from_fs, LocalFileSystem):
            if not isinstance(from_info, from_fs.PATH_CLS):
                from_info = from_fs.PATH_CLS(from_info)
            return to_fs.upload(from_info, to_info)

        if isinstance(to_fs, LocalFileSystem):
            return from_fs.download_file(from_info, to_info)

        with from_fs.open(from_info, mode="rb") as fobj:
            size = from_fs.getsize(from_info)
            return to_fs.upload(fobj, to_info, total=size)
    except OSError as exc:
        # If the target file already exists, we are going to simply
        # ignore the exception (#4992).
        #
        # On Windows, it is not always guaranteed that you'll get
        # FileExistsError (it might be PermissionError or a bare OSError)
        # but all of those exceptions raised from the original
        # FileExistsError so we have a separate check for that.
        if isinstance(exc, FileExistsError) or (
            os.name == "nt"
            and exc.__context__
            and isinstance(exc.__context__, FileExistsError)
        ):
            logger.debug("'%s' file already exists, skipping", to_info)
            return None

        raise
