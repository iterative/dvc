import errno
import logging
import os
from contextlib import nullcontext
from typing import IO, TYPE_CHECKING, List, Optional, Union, cast

from ..utils.fs import as_atomic
from ._callback import DEFAULT_CALLBACK, FsspecCallback
from .base import RemoteActionNotImplemented
from .local import LocalFileSystem

if TYPE_CHECKING:
    from dvc.types import AnyPath

    from .base import AnyFSPath, FileSystem

logger = logging.getLogger(__name__)


def upload(
    fs: "FileSystem",
    lpath: Union["AnyFSPath", IO],
    rpath: "AnyFSPath",
    callback: "FsspecCallback" = DEFAULT_CALLBACK,
    size: int = None,
    atomic: bool = False,
):
    """Atomic version of `fs.put_file`."""
    # no need for `create_parents` here
    # it should depend on `fs.put_file` to handle that.
    context = as_atomic(fs, rpath) if atomic else nullcontext(rpath)
    with context as tmp_file:
        return fs.put_file(lpath, tmp_file, callback=callback, size=size)


def _link(
    link: "str",
    from_fs: "FileSystem",
    from_path: "AnyPath",
    to_fs: "FileSystem",
    to_path: "AnyPath",
) -> None:
    if not isinstance(from_fs, type(to_fs)):
        raise OSError(errno.EXDEV, "can't link across filesystems")

    try:
        func = getattr(to_fs, link)
        func(from_path, to_path)
    except (OSError, AttributeError, RemoteActionNotImplemented) as exc:
        # NOTE: there are so many potential error codes that a link call can
        # throw, that is safer to just catch them all and try another link.
        raise OSError(
            errno.ENOTSUP, f"'{link}' is not supported by {type(from_fs)}"
        ) from exc


def fileobj_size(
    fs: "FileSystem", fileobj: "IO", path: "AnyFSPath"
) -> Optional[int]:
    """Try to deduce size of the file object.

    This does not use `.seek`, instead it tries to deduce that from
    AbstractBufferredFile.size if it exists, otherwise it fetches from
    the filesystem.
    """
    size = cast(Optional[int], getattr(fileobj, "size", None))
    return size or fs.size(path)


def copy(
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
    callback: "FsspecCallback" = DEFAULT_CALLBACK,
    atomic: bool = False,
) -> None:
    logger.debug("Transferring '%s' to '%s'", from_path, to_path)

    if isinstance(from_fs, LocalFileSystem):
        return upload(
            to_fs, from_path, to_path, callback=callback, atomic=atomic
        )

    if isinstance(to_fs, LocalFileSystem):
        return from_fs.download_file(
            from_path, to_path, callback=callback, atomic=atomic
        )

    with from_fs.open(from_path, mode="rb") as fobj:
        size = fileobj_size(from_fs, fobj, from_path)
        return upload(
            to_fs, fobj, to_path, callback=callback, size=size, atomic=atomic
        )


def _try_links(
    links: List["str"],
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
    callback: "FsspecCallback" = None,
    atomic: bool = False,
) -> None:
    error = None
    while links:
        link = links[0]

        if link == "copy":
            with FsspecCallback.as_tqdm_callback(
                callback,
                desc=from_fs.path.name(from_path),
                bytes=True,
                total=-1,
            ) as cb:
                return copy(
                    from_fs,
                    from_path,
                    to_fs,
                    to_path,
                    callback=cb,
                    atomic=atomic,
                )

        try:
            return _link(link, from_fs, from_path, to_fs, to_path)
        except OSError as exc:
            if exc.errno not in [errno.ENOTSUP, errno.EXDEV]:
                raise
            error = exc

        del links[0]

    raise OSError(
        errno.ENOTSUP, "no more link types left to try out"
    ) from error


def transfer(
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
    hardlink: bool = False,
    links: List["str"] = None,
    atomic: bool = False,
) -> None:
    try:
        assert not (hardlink and links)
        if hardlink:
            links = links or ["reflink", "hardlink", "copy"]
        else:
            links = links or ["reflink", "copy"]
        _try_links(links, from_fs, from_path, to_fs, to_path, atomic=atomic)
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
            logger.debug("'%s' file already exists, skipping", to_path)
            return None

        raise


def _test_link(
    link: "str",
    from_fs: "FileSystem",
    from_file: "AnyFSPath",
    to_fs: "FileSystem",
    to_file: "AnyFSPath",
) -> bool:
    try:
        _try_links([link], from_fs, from_file, to_fs, to_file)
    except OSError:
        logger.debug("", exc_info=True)
        return False

    try:
        _is_link_func = getattr(to_fs, f"is_{link}")
        return _is_link_func(to_file)
    except AttributeError:
        pass

    return True


def test_links(
    links: List["str"],
    from_fs: "FileSystem",
    from_path: "AnyFSPath",
    to_fs: "FileSystem",
    to_path: "AnyFSPath",
) -> List["AnyFSPath"]:
    from dvc.utils import tmp_fname

    from_file = from_fs.path.join(from_path, tmp_fname())
    to_file = to_fs.path.join(
        to_fs.path.parent(to_path),
        tmp_fname(),
    )

    from_fs.makedirs(from_fs.path.parent(from_file))
    with from_fs.open(from_file, "wb") as fobj:
        fobj.write(b"test")
    to_fs.makedirs(to_fs.path.parent(to_file))

    ret = []
    try:
        for link in links:
            try:
                if _test_link(link, from_fs, from_file, to_fs, to_file):
                    ret.append(link)
            finally:
                to_fs.remove(to_file)
    finally:
        from_fs.remove(from_file)

    return ret
