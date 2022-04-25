import contextlib
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partialmethod
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
from dvc.ui import ui
from dvc.utils import tmp_fname
from dvc.utils.fs import makedirs, move

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

    # Needed for some providers, and http open()
    CHUNK_SIZE = 64 * 1024 * 1024  # 64 MiB

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
        **kwargs,
    ) -> None:
        size = kwargs.get("size")
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
        total: int = None,
        desc: str = None,
        callback=None,
        no_progress_bar: bool = False,
        **pbar_args: Any,
    ):
        if not hasattr(from_info, "read"):
            from .local import localfs

            desc = desc or localfs.path.name(from_info)
            logger.debug("Uploading '%s' to '%s'", from_info, to_info)

        stack = contextlib.ExitStack()
        if not callback:
            pbar = ui.progress(
                desc=desc,
                disable=no_progress_bar,
                bytes=True,
                total=total or -1,
                **pbar_args,
            )
            stack.enter_context(pbar)
            callback = pbar.as_callback()

        with stack:
            if total:
                callback.set_size(total)
            return self.put_file(
                from_info, to_info, callback=callback, size=total
            )

    def download(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        name: str = None,
        callback=None,
        no_progress_bar: bool = False,
        jobs: int = None,
        _only_file: bool = False,
        **kwargs: Any,
    ):
        from .local import localfs

        if not hasattr(self, "get_file"):
            raise RemoteActionNotImplemented("get_file", self.scheme)

        download_dir = not _only_file and self.isdir(from_info)

        desc = name or localfs.path.name(to_info)
        stack = contextlib.ExitStack()
        if not callback:
            pbar_kwargs = {"unit": "Files"} if download_dir else {}
            pbar = ui.progress(
                total=-1,
                desc="Downloading directory" if download_dir else desc,
                bytes=not download_dir,
                disable=no_progress_bar,
                **pbar_kwargs,
            )
            stack.enter_context(pbar)
            callback = pbar.as_callback()

        with stack:
            if download_dir:
                return self._download_dir(
                    from_info, to_info, callback=callback, jobs=jobs, **kwargs
                )
            return self._download_file(from_info, to_info, callback=callback)

    download_file = partialmethod(download, _only_file=True)

    def _download_dir(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback: FsspecCallback = DEFAULT_CALLBACK,
        jobs: int = None,
        **kwargs,
    ):
        from .local import localfs

        from_infos = list(self.find(from_info, **kwargs))
        if not from_infos:
            makedirs(to_info, exist_ok=True)
            return None

        to_infos = (
            localfs.path.join(to_info, *self.path.relparts(info, from_info))
            for info in from_infos
        )
        callback.set_size(len(from_infos))

        download_files = callback.wrap_fn(self._download_file)
        max_workers = jobs or self.jobs
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(download_files, from_info, to_info)
                for from_info, to_info in zip(from_infos, to_infos)
            ]

            # NOTE: unlike pulling/fetching cache, where we need to
            # download everything we can, not raising an error here might
            # turn very ugly, as the user might think that he has
            # downloaded a complete directory, while having a partial one,
            # which might cause unexpected results in his pipeline.
            for future in as_completed(futures):
                # NOTE: executor won't let us raise until all futures that
                # it has are finished, so we need to cancel them ourselves
                # before re-raising.
                exc = future.exception()
                if exc:
                    for entry in futures:
                        entry.cancel()
                    raise exc

    def _download_file(
        self,
        from_info: AnyFSPath,
        to_info: AnyFSPath,
        callback=DEFAULT_CALLBACK,
    ) -> None:
        from .local import localfs

        makedirs(localfs.path.parent(to_info), exist_ok=True)
        tmp_file = tmp_fname(to_info)

        logger.debug("Downloading '%s' to '%s'", from_info, to_info)
        try:
            # noqa, pylint: disable=no-member
            self.get_file(from_info, tmp_file, callback=callback)
        except Exception:  # pylint: disable=broad-except
            # do we need to rollback makedirs for previously not-existing
            # directories?
            with contextlib.suppress(FileNotFoundError):
                os.unlink(tmp_file)
            raise

        move(tmp_file, to_info)


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
