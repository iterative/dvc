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

from dvc.exceptions import DvcException
from dvc.utils.fs import as_atomic, makedirs
from dvc.utils.threadpool import ThreadPoolExecutor

from ._callback import DEFAULT_CALLBACK, FsspecCallback

if TYPE_CHECKING:
    from fsspec.spec import AbstractFileSystem
    from typing_extensions import Literal


logger = logging.getLogger(__name__)


FSPath = str
AnyFSPath = str

# An info() entry, might evolve to a TypedDict
# in the future (e.g for properly type 'size' etc).
Entry = Dict[str, Any]


class RemoteActionNotImplemented(DvcException):
    def __init__(self, action, scheme):
        m = f"{action} is not supported for {scheme} remotes"
        super().__init__(m)


class RemoteMissingDepsError(DvcException):
    pass


class FileSystem:
    sep = "/"

    scheme = "base"
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

        return Path(self.sep)

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
        from ..scheme import Schemes
        from ..utils import format_link
        from ..utils.pkg import PKG

        missing = self.get_missing_deps()
        if not missing:
            return

        url = kwargs.get("url", f"{self.scheme}://")

        scheme = self.scheme
        if scheme == Schemes.WEBDAVS:
            scheme = Schemes.WEBDAV

        by_pkg = {
            "pip": f"pip install 'dvc[{scheme}]'",
            "conda": f"conda install -c conda-forge dvc-{scheme}",
        }

        cmd = by_pkg.get(PKG)
        if cmd:
            link = format_link("https://dvc.org/doc/install")
            hint = (
                f"To install dvc with those dependencies, run:\n"
                "\n"
                f"\t{cmd}\n"
                "\n"
                f"See {link} for more info."
            )
        else:
            link = format_link("https://github.com/iterative/dvc/issues")
            hint = f"Please report this bug to {link}. Thank you!"

        raise RemoteMissingDepsError(
            f"URL '{url}' is supported but requires these missing "
            f"dependencies: {missing}. {hint}"
        )

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
        return self.fs.open(path, mode=mode, encoding=encoding, **kwargs)

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

    def exists(self, path: AnyFSPath) -> bool:
        return self.fs.exists(path)

    def lexists(self, path: AnyFSPath) -> bool:
        return self.fs.lexists(path)

    def symlink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.symlink(from_info, to_info)
        except AttributeError:
            raise RemoteActionNotImplemented("symlink", self.scheme)

    def hardlink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.hardlink(from_info, to_info)
        except AttributeError:
            raise RemoteActionNotImplemented("hardlink", self.scheme)

    def reflink(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        try:
            return self.fs.reflink(from_info, to_info)
        except AttributeError:
            raise RemoteActionNotImplemented("reflink", self.scheme)

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
        return self.is_symlink(path) or self.is_hardlink(path)

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

    def move(self, from_info: AnyFSPath, to_info: AnyFSPath) -> None:
        self.fs.move(from_info, to_info)

    def remove(self, path: AnyFSPath) -> None:
        self.fs.rm(path, recursive=True)

    def info(self, path: AnyFSPath) -> "Entry":
        return self.fs.info(path)

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
        self.fs.makedirs(path, exist_ok=kwargs.pop("exist_ok", True))

    def put_file(
        self,
        from_file: Union[AnyFSPath, IO],
        to_info: AnyFSPath,
        callback: FsspecCallback = DEFAULT_CALLBACK,
        size: int = None,
        **kwargs,
    ) -> None:
        if size:
            callback.set_size(size)
        if hasattr(from_file, "read"):
            stream = callback.wrap_attr(cast("IO", from_file))
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
        callback: FsspecCallback = DEFAULT_CALLBACK,
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

    def size(self, path: AnyFSPath) -> Optional[int]:
        return self.fs.size(path)

    # pylint: enable=unused-argument

    def upload(
        self,
        from_info: Union[AnyFSPath, IO],
        to_info: AnyFSPath,
        size: int = None,
        callback: FsspecCallback = None,
    ):
        from .local import localfs

        if not hasattr(from_info, "read"):
            logger.debug("Uploading '%s' to '%s'", from_info, to_info)
            desc = localfs.path.name(from_info)
        else:
            desc = self.path.name(to_info)

        with FsspecCallback.as_tqdm_callback(
            callback,
            desc=desc,
            bytes=True,
            total=size or -1,
        ) as cb:
            return self.put_file(from_info, to_info, callback=cb, size=size)

    def download(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: "FsspecCallback" = None,
        jobs: int = None,
    ):
        if not self.isdir(from_info):
            return self.download_file(from_info, to_info, callback=callback)

        from .local import localfs

        pairs = {
            info: localfs.path.join(
                to_info, *self.path.relparts(info, from_info)
            )
            for info in self.find(from_info)
        }
        if not pairs:
            makedirs(to_info, exist_ok=True)
            return

        with FsspecCallback.as_tqdm_callback(
            callback,
            total=-1,
            desc=f"Downloading directory {self.path.name(from_info)}",
            unit="files",
        ) as cb:
            cb.set_size(len(pairs))
            download_files = cb.wrap_and_branch(self.download_file, fs=self)
            max_workers = jobs or self.jobs
            # NOTE: unlike pulling/fetching cache, where we need to
            # download everything we can, not raising an error here might
            # turn very ugly, as the user might think that he has
            # downloaded a complete directory, while having a partial one,
            # which might cause unexpected results in his pipeline.
            with ThreadPoolExecutor(
                max_workers=max_workers, cancel_on_error=True
            ) as executor:
                list(
                    executor.imap_unordered(
                        lambda args: download_files(*args), pairs.items()
                    )
                )

    def download_file(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: FsspecCallback = None,
    ):
        from .local import localfs

        with FsspecCallback.as_tqdm_callback(
            callback,
            total=-1,
            desc=self.path.name(from_info),
            bytes=True,
        ) as cb:
            with as_atomic(localfs, to_info, create_parents=True) as tmp_file:
                self.get_file(from_info, tmp_file, callback=cb)


class ObjectFileSystem(FileSystem):  # pylint: disable=abstract-method
    TRAVERSE_PREFIX_LEN = 3

    def makedirs(self, path: AnyFSPath, **kwargs: Any) -> None:
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
