import os
import pathlib
import sys
from itertools import chain
from typing import TYPE_CHECKING, overload

from .exceptions import OutputNotFoundError, PathMissingError
from .types import OptStr

if TYPE_CHECKING:
    from io import (
        BufferedRandom,
        BufferedReader,
        BufferedWriter,
        FileIO,
        TextIOWrapper,
    )
    from typing import IO, Any, BinaryIO, Generator, Union

    from _typeshed import (
        OpenBinaryMode,
        OpenBinaryModeReading,
        OpenBinaryModeUpdating,
        OpenBinaryModeWriting,
        OpenTextMode,
        OpenTextModeReading,
    )
    from typing_extensions import Literal

    from .fs.repo import RepoFileSystem


def _unsupported(method: str):
    def wrapped(*args, **kwargs):
        raise NotImplementedError(f"{method}() is unsupported.")

    return wrapped


class _PathNotSupportedMixin:
    samefile = _unsupported("samefile")
    absolute = _unsupported("absolute")
    resolve = _unsupported("resolve")
    stat = _unsupported("stat")
    owner = _unsupported("owner")
    group = _unsupported("group")
    write_bytes = _unsupported("write_bytes")
    write_text = _unsupported("write_text")
    touch = _unsupported("touch")
    mkdir = _unsupported("mkdir")
    chmod = _unsupported("chmod")
    unlink = _unsupported("unlink")
    rmdir = _unsupported("rmdir")
    lstat = _unsupported("lstat")
    rename = _unsupported("rename")
    replace = _unsupported("replace")
    symlink_to = _unsupported("symlink_to")
    is_mount = _unsupported("is_mount")


class PureRepoPath(pathlib.PurePath):
    # pylint: disable=protected-access
    _flavour = (
        pathlib._WindowsFlavour()  # type: ignore[attr-defined]
        if os.name == "nt"
        else pathlib._PosixFlavour()  # type: ignore[attr-defined]
    )
    __slots__ = ()


class RepoPath(  # lgtm[py/conflicting-attributes]
    # pylint:disable=abstract-method
    _PathNotSupportedMixin,
    PureRepoPath,
    pathlib.Path,
):
    _fs: "RepoFileSystem"

    scheme = "local"
    __slots__ = ("_fs",)

    def __new__(cls, *args, **kwargs):
        args_list = list(args)
        repo_path = args_list.pop(0)
        kw = {"init": False} if sys.version_info < (3, 10) else {}
        self = super()._from_parts(  # pylint: disable=unexpected-keyword-arg
            args, **kw
        )
        if isinstance(repo_path, RepoPath):
            # pylint: disable=protected-access
            kwargs["fs"] = kwargs.get("fs") or repo_path._fs
        self._init(*args, **kwargs)  # pylint: disable=no-member
        return self

    def _from_parsed_parts(self, *args, **kwargs):
        new = super()._from_parsed_parts(*args, **kwargs)
        # pylint: disable=protected-access, assigning-non-slot
        new._fs = self._fs
        return new

    def _init(  # pylint: disable=arguments-differ
        self, *args, template=None, fs=None
    ):
        self._fs = fs  # pylint: disable=disable=assigning-non-slot
        if sys.version_info > (3, 10):
            return
        super()._init(template)  # pylint: disable=no-member

    def url(self, remote: str = None) -> str:
        fs = self._fs
        fs_path = fs.path.join(fs.root_dir, str(self))
        try:
            metadata = fs.metadata(fs_path)
        except FileNotFoundError:
            # pylint: disable=protected-access
            raise PathMissingError(str(self), fs._main_repo)

        if not metadata.is_dvc:
            raise OutputNotFoundError(str(self), metadata.repo)

        cloud = metadata.repo.cloud
        md5 = metadata.repo.dvcfs.info(fs_path)["md5"]
        return cloud.get_url_for(remote, checksum=md5)

    def exists(self) -> bool:
        return self._fs.exists(self)

    def is_dir(self) -> bool:
        return self._fs.isdir(self)

    def is_file(self) -> bool:
        return self._fs.isfile(self)

    @overload
    def read(
        self,
        mode: "OpenTextModeReading",
        remote: str = None,
        encoding: str = None,
        errors: str = None,
    ) -> str:
        ...

    @overload
    def read(
        self,
        mode: "OpenBinaryModeReading",
        remote: str = None,
        encoding: str = None,
        errors: str = None,
    ) -> bytes:
        ...

    @overload
    def read(
        self,
        mode: str = ...,
        remote: str = None,
        encoding: str = None,
        errors: str = None,
    ) -> "Union[str, bytes]":
        ...

    def read(
        self,
        mode: str = "r",
        remote: str = None,
        encoding: str = None,
        errors: str = None,
    ):
        with self.open(  # pylint: disable=not-context-manager
            remote=remote, mode=mode, encoding=encoding, errors=errors
        ) as f:
            return f.read()

    def read_bytes(  # pylint: disable=arguments-differ
        self, remote: str = None
    ) -> bytes:
        return self.read(remote=remote, mode="rb")

    def read_text(  # pylint: disable=arguments-differ
        self, encoding: str = None, errors: str = None, remote: str = None
    ) -> str:
        return self.read(
            mode="r", encoding=encoding, errors=errors, remote=remote
        )

    def iterdir(self) -> "Generator[RepoPath, None, None]":
        def onerror(exc):
            raise exc

        repo_walk = self._fs.walk(self, onerror=onerror, dvcfiles=True)
        for _, dirs, files in repo_walk:
            yield from (self / entry for entry in chain(files, dirs))
            break

    # NOTE: keep in sync with Pathlib.open typehints
    # pylint: disable=arguments-differ
    @overload
    def open(
        self,
        mode: "OpenTextMode" = ...,
        buffering: int = ...,
        encoding: OptStr = ...,
        errors: OptStr = ...,
        newline: OptStr = ...,
        remote: OptStr = ...,
    ) -> "TextIOWrapper":
        ...

    # Unbuffered binary mode: returns a FileIO
    @overload
    def open(
        self,
        mode: "OpenBinaryMode",
        buffering: "Literal[0]",
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
        remote: OptStr = ...,
    ) -> "FileIO":
        ...

    # Buffering is on: return BufferedRandom, BufferedReader, or BufferedWriter
    @overload
    def open(
        self,
        mode: "OpenBinaryModeUpdating",
        buffering: "Literal[-1, 1]" = ...,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
        remote: OptStr = ...,
    ) -> "BufferedRandom":
        ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeWriting",
        buffering: "Literal[-1, 1]" = ...,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
        remote: OptStr = ...,
    ) -> "BufferedWriter":
        ...

    @overload
    def open(
        self,
        mode: "OpenBinaryModeReading",
        buffering: "Literal[-1, 1]" = ...,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
        remote: OptStr = ...,
    ) -> "BufferedReader":
        ...

    # Buffering cannot be determined: fall back to BinaryIO
    @overload
    def open(
        self,
        mode: "OpenBinaryMode",
        buffering: int,
        encoding: None = ...,
        errors: None = ...,
        newline: None = ...,
        remote: OptStr = ...,
    ) -> "BinaryIO":
        ...

    # Fallback if mode is not specified
    @overload
    def open(
        self,
        mode: str,
        buffering: int = ...,
        encoding: OptStr = ...,
        errors: OptStr = ...,
        newline: OptStr = ...,
        remote: OptStr = ...,
    ) -> "IO[Any]":
        ...

    def open(
        self,
        mode="r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        remote=None,
    ):
        assert buffering == -1
        assert errors is None
        assert newline is None
        assert mode in {"rt", "tr", "r", "rb", "br"}

        main_repo = self._fs._main_repo  # pylint: disable=protected-access
        return main_repo.open_by_relpath(
            self, mode=mode, encoding=encoding, remote=remote
        )

    # pylint: enable=arguments-differ
