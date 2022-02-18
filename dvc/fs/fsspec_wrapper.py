import os
import shutil
from typing import IO, TYPE_CHECKING, Any, Dict, Iterator, Optional, overload

from funcy import cached_property
from tqdm.utils import CallbackIOWrapper

from ._callback import DEFAULT_CALLBACK
from .base import FileSystem

FSPath = str
AnyFSPath = str

if TYPE_CHECKING:
    from typing_extensions import Literal


# An info() entry, might evolve to a TypedDict
# in the future (e.g for properly type 'size' etc).
Entry = Dict[str, Any]


# pylint: disable=no-member
class FSSpecWrapper(FileSystem):
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fs_args = {"skip_instance_cache": True}
        self.fs_args.update(self._prepare_credentials(**kwargs))

    @staticmethod
    def _get_kwargs_from_urls(urlpath: str) -> "Dict[str, Any]":
        from fsspec.utils import infer_storage_options

        options = infer_storage_options(urlpath)
        options.pop("path", None)
        options.pop("protocol", None)
        return options

    @cached_property
    def fs(self):
        raise NotImplementedError

    def _prepare_credentials(
        self, **config: Dict[str, Any]  # pylint: disable=unused-argument
    ) -> Dict[str, Any]:
        """Prepare the arguments for authentication to the
        host filesystem"""
        return {}

    def isdir(self, path: AnyFSPath) -> bool:
        return self.fs.isdir(path)

    def isfile(self, path: AnyFSPath) -> bool:
        return self.fs.isfile(path)

    def is_empty(self, path: AnyFSPath) -> bool:
        entry = self.info(path)
        if entry["type"] == "directory":
            return not self.fs.ls(path)
        return entry["size"] == 0

    def open(
        self,
        path: AnyFSPath,
        mode: str = "r",
        encoding: Optional[str] = None,
        **kwargs,
    ) -> "IO":  # pylint: disable=arguments-differ
        return self.fs.open(path, mode=mode)

    def checksum(self, path: AnyFSPath) -> str:
        return self.fs.checksum(path)

    def copy(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        self.makedirs(self.path.parent(to_info))
        self.fs.copy(from_info, to_info)

    def exists(self, path: AnyFSPath) -> bool:
        return self.fs.exists(path)

    @overload
    def ls(
        self, path: AnyFSPath, detail: "Literal[True]"
    ) -> "Iterator[Entry]":
        ...

    @overload
    def ls(self, path: AnyFSPath, detail: "Literal[False]") -> Iterator[str]:
        ...

    def ls(self, path, detail=False):
        yield from self.fs.ls(path, detail=detail)

    def find(self, path, prefix=None):
        yield from self.fs.find(path)

    def move(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        self.fs.move(from_info, to_info)

    def remove(self, path: AnyFSPath) -> None:
        self.fs.rm_file(path)

    def info(self, path: AnyFSPath) -> "Entry":
        return self.fs.info(path)

    def makedirs(self, path: AnyFSPath, **kwargs) -> None:
        self.fs.makedirs(path, exist_ok=kwargs.pop("exist_ok", True))

    def put_file(
        self,
        from_file: AnyFSPath,
        to_info: AnyFSPath,
        callback: Any = DEFAULT_CALLBACK,
        **kwargs,
    ) -> None:
        self.fs.put_file(from_file, to_info, callback=callback, **kwargs)
        self.fs.invalidate_cache(self.path.parent(to_info))

    def get_file(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: Any = DEFAULT_CALLBACK,
        **kwargs,
    ) -> None:
        self.fs.get_file(from_info, to_info, callback=callback, **kwargs)

    def upload_fobj(self, fobj: IO, to_info: AnyFSPath, **kwargs) -> None:
        self.makedirs(self.path.parent(to_info))
        with self.open(to_info, "wb") as fdest:
            shutil.copyfileobj(
                fobj,
                fdest,
                length=getattr(fdest, "blocksize", None),  # type: ignore
            )

    def walk(
        self,
        top: AnyFSPath,
        topdown: bool = True,
        **kwargs: Any,
    ):
        return self.fs.walk(top, topdown=topdown, **kwargs)


# pylint: disable=abstract-method
class ObjectFSWrapper(FSSpecWrapper):
    TRAVERSE_PREFIX_LEN = 3

    def makedirs(self, path: AnyFSPath, **kwargs) -> None:
        # For object storages make this method a no-op. The original
        # fs.makedirs() method will only check if the bucket exists
        # and create if it doesn't though we don't want to support
        # that behavior, and the check will cost some time so we'll
        # simply ignore all mkdir()/makedirs() calls.
        return None

    def _isdir(self, path: AnyFSPath) -> bool:
        # Directory in object storages are interpreted differently
        # among different fsspec providers, so this logic is a temporary
        # measure for us to adapt as of now. It checks whether it is a
        # directory (as in a prefix with contents) or whether it is an empty
        # file where it's name ends with a forward slash

        entry = self.info(path)
        return entry["type"] == "directory" or (
            entry["size"] == 0
            and entry["type"] == "file"
            and entry["name"].endswith("/")
        )

    def isdir(self, path: AnyFSPath) -> bool:
        try:
            return self._isdir(path)
        except FileNotFoundError:
            return False

    def isfile(self, path: AnyFSPath) -> bool:
        try:
            return not self._isdir(path)
        except FileNotFoundError:
            return False

    def find(self, path, prefix=None):
        if prefix:
            with_prefix = self.path.parent(path)
            files = self.fs.find(with_prefix, prefix=self.path.parts(path)[-1])
        else:
            with_prefix = path
            files = self.fs.find(path)

        # When calling find() on a file, it returns the same file in a list.
        # For object-based storages, the same behavior applies to empty
        # directories since they are represented as files. This condition
        # checks whether we should yield an empty list (if it is an empty
        # directory) or just yield the file itself.
        if len(files) == 1 and files[0] == with_prefix and self.isdir(path):
            return None

        yield from files


# pylint: disable=arguments-differ
class NoDirectoriesMixin:
    def isdir(self, *args, **kwargs):
        return False

    def isfile(self, *args, **kwargs):
        return True

    def find(self, *args, **kwargs):
        raise NotImplementedError

    def walk(self, *args, **kwargs):
        raise NotImplementedError

    def ls(self, *args, **kwargs):
        raise NotImplementedError


class CallbackMixin:
    """Provides callback support for the filesystem that don't support yet."""

    def put_file(
        self,
        from_file,
        to_info,
        callback=DEFAULT_CALLBACK,
        **kwargs,
    ):
        """Add compatibility support for Callback."""
        # pylint: disable=protected-access
        self.makedirs(self.path.parent(to_info))
        size = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            callback.set_size(size)
            wrapped = CallbackIOWrapper(callback.relative_update, fobj)
            self.upload_fobj(wrapped, to_info)
            self.fs.invalidate_cache(self.path.parent(to_info))

    def get_file(
        self,
        from_info,
        to_info,
        callback=DEFAULT_CALLBACK,
        **kwargs,
    ):
        # pylint: disable=protected-access
        total: int = self.getsize(from_info)
        if total:
            callback.set_size(total)

        with self.open(from_info, "rb") as fobj, open(to_info, "wb") as fdest:
            wrapped = CallbackIOWrapper(callback.relative_update, fobj)
            shutil.copyfileobj(wrapped, fdest, length=fobj.blocksize)
