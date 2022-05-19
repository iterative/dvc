import datetime
import logging
import os
import shutil
from multiprocessing import cpu_count
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
    cast,
    overload,
)

from funcy import cached_property

from ..executors import ThreadPoolExecutor
from .callbacks import DEFAULT_CALLBACK, Callback
from .errors import RemoteMissingDepsError

if TYPE_CHECKING:
    from typing import BinaryIO, TextIO

    from fsspec.spec import AbstractFileSystem
    from typing_extensions import Literal


logger = logging.getLogger(__name__)


FSPath = str
AnyFSPath = str

# An info() entry, might evolve to a TypedDict
# in the future (e.g for properly type 'size' etc).
Entry = Dict[str, Any]


class LinkError(OSError):
    def __init__(self, link: str, fs: "FileSystem", path: str) -> None:
        import errno

        super().__init__(
            errno.EPERM,
            f"{link} is not supported for {fs.protocol} by {type(fs)}",
            path,
        )


class FileSystem:
    sep = "/"

    protocol = "base"
    REQUIRES: ClassVar[Dict[str, str]] = {}
    _JOBS = 4 * cpu_count()

    HASH_JOBS = max(1, min(4, cpu_count() // 2))
    LIST_OBJECT_PAGE_SIZE = 1000
    TRAVERSE_WEIGHT_MULTIPLIER = 5
    TRAVERSE_PREFIX_LEN = 2
    TRAVERSE_THRESHOLD_SIZE = 500000
    CAN_TRAVERSE = True

    PARAM_CHECKSUM: ClassVar[Optional[str]] = None

    def __init__(self, **kwargs):
        self._check_requires(**kwargs)

        self.jobs = kwargs.get("jobs") or self._JOBS
        self.hash_jobs = kwargs.get("checksum_jobs") or self.HASH_JOBS
        self._config = kwargs
        self.fs_args = {"skip_instance_cache": True}
        self.fs_args.update(self._prepare_credentials(**kwargs))

    @property
    def config(self) -> Dict[str, Any]:
        return self._config

    @cached_property
    def path(self):
        from .path import Path

        def _getcwd():
            return self.fs.root_marker

        return Path(self.sep, getcwd=_getcwd)

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        return path

    def unstrip_protocol(self, path: str) -> str:
        return path

    @cached_property
    def fs(self) -> "AbstractFileSystem":
        raise NotImplementedError

    @staticmethod
    def _get_kwargs_from_urls(urlpath: str) -> "Dict[str, Any]":
        from fsspec.utils import infer_storage_options

        options = infer_storage_options(urlpath)
        options.pop("path", None)
        options.pop("protocol", None)
        return options

    def _prepare_credentials(
        self, **config: Dict[str, Any]  # pylint: disable=unused-argument
    ) -> Dict[str, Any]:
        """Prepare the arguments for authentication to the
        host filesystem"""
        return {}

    @classmethod
    def get_missing_deps(cls) -> List[str]:
        import importlib

        missing: List[str] = []
        for package, module in cls.REQUIRES.items():
            try:
                importlib.import_module(module)
            except ImportError:
                missing.append(package)

        return missing

    def _check_requires(self, **kwargs):
        from .scheme import Schemes

        missing = self.get_missing_deps()
        if not missing:
            return

        proto = self.protocol
        if proto == Schemes.WEBDAVS:
            proto = Schemes.WEBDAV

        url = kwargs.get("url", f"{self.protocol}://")
        raise RemoteMissingDepsError(self, proto, url, missing)

    def isdir(self, path: AnyFSPath) -> bool:
        return self.fs.isdir(path)

    def isfile(self, path: AnyFSPath) -> bool:
        return self.fs.isfile(path)

    def is_empty(self, path: AnyFSPath) -> bool:
        entry = self.info(path)
        if entry["type"] == "directory":
            return not self.fs.ls(path)
        return entry["size"] == 0

    @overload
    def open(
        self,
        path: AnyFSPath,
        mode: "Literal['rb', 'br', 'wb']",
        **kwargs: Any,
    ) -> "BinaryIO":  # pylint: disable=arguments-differ
        return self.open(path, mode, **kwargs)

    @overload
    def open(
        self,
        path: AnyFSPath,
        mode: "Literal['r', 'rt', 'w']",
        **kwargs: Any,
    ) -> "TextIO":  # pylint: disable=arguments-differ
        ...

    def open(
        self,
        path: AnyFSPath,
        mode: str = "r",
        **kwargs: Any,
    ) -> "IO[Any]":  # pylint: disable=arguments-differ
        if "b" in mode:
            kwargs.pop("encoding", None)
        return self.fs.open(path, mode=mode, **kwargs)

    def read_block(
        self,
        path: AnyFSPath,
        offset: int,
        length: int,
        delimiter: bytes = None,
    ) -> bytes:
        return self.fs.read_block(path, offset, length, delimiter=delimiter)

    def cat(
        self,
        path: Union[AnyFSPath, List[AnyFSPath]],
        recursive: bool = False,
        on_error: "Literal['raise', 'omit', 'return']" = "raise",
        **kwargs: Any,
    ) -> Union[bytes, Dict[AnyFSPath, bytes]]:
        return self.fs.cat(
            path, recursive=recursive, on_error=on_error, **kwargs
        )

    def cat_ranges(
        self,
        paths: List[AnyFSPath],
        starts: List[int],
        ends: List[int],
        max_gap: int = None,
        **kwargs,
    ) -> List[bytes]:
        return self.fs.cat_ranges(
            paths, starts, ends, max_gap=max_gap, **kwargs
        )

    def cat_file(
        self,
        path: AnyFSPath,
        start: int = None,
        end: int = None,
        **kwargs: Any,
    ) -> bytes:
        return self.fs.cat_file(path, start=start, end=end, **kwargs)

    def head(self, path: AnyFSPath, size: int = 1024) -> bytes:
        return self.fs.head(path, size=size)

    def tail(self, path: AnyFSPath, size: int = 1024) -> bytes:
        return self.fs.tail(path, size=size)

    def pipe_file(self, path: AnyFSPath, value: bytes, **kwargs: Any) -> None:
        return self.fs.pipe_file(path, value, **kwargs)

    def pipe(
        self,
        path: Union[AnyFSPath, Dict[AnyFSPath, bytes]],
        value: Optional[bytes] = None,
        **kwargs: Any,
    ) -> None:
        return self.fs.pipe(path, value=value, **kwargs)

    def touch(
        self, path: AnyFSPath, truncate: bool = True, **kwargs: Any
    ) -> None:
        return self.fs.touch(path, truncate=truncate, **kwargs)

    def checksum(self, path: AnyFSPath) -> str:
        return self.fs.checksum(path)

    def copy(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        self.makedirs(self.path.parent(to_info))
        self.fs.copy(from_info, to_info)

    def cp_file(
        self, from_info: AnyFSPath, to_info: AnyFSPath, **kwargs: Any
    ) -> None:
        self.fs.cp_file(from_info, to_info, **kwargs)

    def exists(self, path: AnyFSPath) -> bool:
        return self.fs.exists(path)

    def lexists(self, path: AnyFSPath) -> bool:
        return self.fs.lexists(path)

    def symlink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.symlink(from_info, to_info)
        except AttributeError:
            raise LinkError("symlink", self, from_info)

    def hardlink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.hardlink(from_info, to_info)
        except AttributeError:
            raise LinkError("symlink", self, from_info)

    def reflink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.reflink(from_info, to_info)
        except AttributeError:
            raise LinkError("symlink", self, from_info)

    def is_symlink(self, path: AnyFSPath) -> bool:
        try:
            return self.fs.is_symlink(path)
        except AttributeError:
            return False

    def is_hardlink(self, path: AnyFSPath) -> bool:
        try:
            return self.fs.is_hardlink(path)
        except AttributeError:
            return False

    def iscopy(self, path: AnyFSPath) -> bool:
        return not (self.is_symlink(path) or self.is_hardlink(path))

    @overload
    def ls(
        self, path: AnyFSPath, detail: "Literal[True]"
    ) -> "Iterator[Entry]":
        ...

    @overload
    def ls(self, path: AnyFSPath, detail: "Literal[False]") -> Iterator[str]:
        ...

    def ls(self, path, detail=False, **kwargs):
        return self.fs.ls(path, detail=detail)

    def find(
        self,
        path: AnyFSPath,
        prefix: bool = False,  # pylint: disable=unused-argument
    ) -> Iterator[str]:
        yield from self.fs.find(path)

    def mv(
        self, from_info: AnyFSPath, to_info: AnyFSPath, **kwargs: Any
    ) -> None:
        self.fs.mv(from_info, to_info)

    move = mv

    def rmdir(self, path: AnyFSPath) -> None:
        self.fs.rmdir(path)

    def rm_file(self, path: AnyFSPath) -> None:
        self.fs.rm_file(path)

    def rm(self, path: AnyFSPath) -> None:
        self.fs.rm(path, recursive=True)

    remove = rm

    def info(self, path: AnyFSPath) -> "Entry":
        return self.fs.info(path)

    def mkdir(
        self, path: AnyFSPath, create_parents: bool = True, **kwargs: Any
    ) -> None:
        self.fs.mkdir(path, create_parents=create_parents, **kwargs)

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        self.fs.makedirs(path, exist_ok=kwargs.pop("exist_ok", True))

    def put_file(
        self,
        from_file: Union[AnyFSPath, "BinaryIO"],
        to_info: AnyFSPath,
        callback: Callback = DEFAULT_CALLBACK,
        size: int = None,
        **kwargs,
    ) -> None:
        if size:
            callback.set_size(size)
        if hasattr(from_file, "read"):
            stream = callback.wrap_attr(cast("BinaryIO", from_file))
            self.upload_fobj(stream, to_info, size=size)
        else:
            assert isinstance(from_file, str)
            self.fs.put_file(
                os.fspath(from_file), to_info, callback=callback, **kwargs
            )
        self.fs.invalidate_cache(self.path.parent(to_info))

    def get_file(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: Callback = DEFAULT_CALLBACK,
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
        path: AnyFSPath,
        topdown: bool = True,
        **kwargs: Any,
    ):
        return self.fs.walk(path, topdown=topdown, **kwargs)

    def glob(self, path: AnyFSPath, **kwargs: Any):
        return self.fs.glob(path, **kwargs)

    def size(self, path: AnyFSPath) -> Optional[int]:
        return self.fs.size(path)

    def sizes(self, paths: List[AnyFSPath]) -> List[Optional[int]]:
        return self.fs.sizes(paths)

    def du(
        self,
        path: AnyFSPath,
        total: bool = True,
        maxdepth: int = None,
        **kwargs: Any,
    ) -> Union[int, Dict[AnyFSPath, int]]:
        return self.fs.du(path, total=total, maxdepth=maxdepth, **kwargs)

    def put(
        self,
        from_info: Union[AnyFSPath, List[AnyFSPath]],
        to_info: Union[AnyFSPath, List[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        recursive: bool = False,  # pylint: disable=unused-argument
        batch_size: int = None,
    ):
        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            return self.fs.put(
                from_info,
                to_info,
                callback=callback,
                batch_size=jobs,
                recursive=recursive,
            )

        assert not recursive, "not implemented yet"
        from_infos = [from_info] if isinstance(from_info, str) else from_info
        to_infos = [to_info] if isinstance(to_info, str) else to_info

        callback.set_size(len(from_infos))
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            put_file = callback.wrap_and_branch(self.put_file)
            list(executor.imap_unordered(put_file, from_infos, to_infos))

    def get(
        self,
        from_info: Union[AnyFSPath, List[AnyFSPath]],
        to_info: Union[AnyFSPath, List[AnyFSPath]],
        callback: "Callback" = DEFAULT_CALLBACK,
        recursive: bool = False,  # pylint: disable=unused-argument
        batch_size: int = None,
    ) -> None:
        # Currently, the implementation is non-recursive if the paths are
        # provided as a list, and recursive if it's a single path.
        from .implementations.local import localfs

        def get_file(rpath, lpath, **kwargs):
            localfs.makedirs(localfs.path.parent(lpath), exist_ok=True)
            self.fs.get_file(rpath, lpath, **kwargs)

        get_file = callback.wrap_and_branch(get_file)

        if isinstance(from_info, list) and isinstance(to_info, list):
            from_infos: List[AnyFSPath] = from_info
            to_infos: List[AnyFSPath] = to_info
        else:
            assert isinstance(from_info, str)
            assert isinstance(to_info, str)

            if not self.isdir(from_info):
                callback.set_size(1)
                return get_file(from_info, to_info)

            from_infos = list(self.find(from_info))
            if not from_infos:
                return localfs.makedirs(to_info, exist_ok=True)

            to_infos = [
                localfs.path.join(
                    to_info, *self.path.relparts(info, from_info)
                )
                for info in from_infos
            ]

        jobs = batch_size or self.jobs
        if self.fs.async_impl:
            return self.fs.get(
                from_infos,
                to_infos,
                callback=callback,
                batch_size=jobs,
            )

        callback.set_size(len(from_infos))
        executor = ThreadPoolExecutor(max_workers=jobs, cancel_on_error=True)
        with executor:
            list(executor.imap_unordered(get_file, from_infos, to_infos))

    def ukey(self, path: AnyFSPath) -> str:
        return self.fs.ukey(path)

    def created(self, path: AnyFSPath) -> datetime.datetime:
        return self.fs.created(path)

    def modified(self, path: AnyFSPath) -> datetime.datetime:
        return self.fs.modified(path)

    def sign(
        self, path: AnyFSPath, expiration: int = 100, **kwargs: Any
    ) -> str:
        return self.fs.sign(path, expiration=expiration, **kwargs)


class ObjectFileSystem(FileSystem):  # pylint: disable=abstract-method
    TRAVERSE_PREFIX_LEN = 3

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        # For object storages make this method a no-op. The original
        # fs.makedirs() method will only check if the bucket exists
        # and create if it doesn't though we don't want to support
        # that behavior, and the check will cost some time so we'll
        # simply ignore all mkdir()/makedirs() calls.
        return None

    def mkdir(
        self, path: AnyFSPath, create_parents: bool = True, **kwargs: Any
    ) -> None:
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

    def find(self, path: AnyFSPath, prefix: bool = False) -> Iterator[str]:
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
