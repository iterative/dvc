import errno
import logging
import os
from typing import TYPE_CHECKING, List, Optional

from .base import RemoteActionNotImplemented
from .local import LocalFileSystem

if TYPE_CHECKING:
    from .base import FileSystem
    from .types import AnyPath

logger = logging.getLogger(__name__)


def _link(
    link: "AnyPath",
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


def _copy(
    from_fs: "FileSystem",
    from_path: "AnyPath",
    to_fs: "FileSystem",
    to_path: "AnyPath",
) -> None:
    if isinstance(from_fs, LocalFileSystem):
        return to_fs.upload(from_path, to_path)

    if isinstance(to_fs, LocalFileSystem):
        return from_fs.download_file(from_path, to_path)

    with from_fs.open(from_path, mode="rb") as fobj:
        size = from_fs.getsize(from_path)
        return to_fs.upload(fobj, to_path, total=size)


def _try_links(
    links: List["AnyPath"],
    from_fs: "FileSystem",
    from_path: "AnyPath",
    to_fs: "FileSystem",
    to_path: "AnyPath",
) -> None:
    error = None
    while links:
        link = links[0]

        if link == "copy":
            return _copy(from_fs, from_path, to_fs, to_path)

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
    from_path: "AnyPath",
    to_fs: "FileSystem",
    to_path: "AnyPath",
    hardlink: bool = False,
    links: Optional[List["AnyPath"]] = None,
) -> None:
    try:
        assert not (hardlink and links)
        if hardlink:
            links = links or ["reflink", "hardlink", "copy"]
        else:
            links = links or ["reflink", "copy"]
        _try_links(links, from_fs, from_path, to_fs, to_path)
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
    link: "AnyPath",
    from_fs: "FileSystem",
    from_file: "AnyPath",
    to_fs: "FileSystem",
    to_file: "AnyPath",
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
    links: List["AnyPath"],
    from_fs: "FileSystem",
    from_path: "AnyPath",
    to_fs: "FileSystem",
    to_path: "AnyPath",
) -> List["AnyPath"]:
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
