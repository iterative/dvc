import contextlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partialmethod
from multiprocessing import cpu_count
from typing import ClassVar, Dict, Optional

from funcy import cached_property
from tqdm.utils import CallbackIOWrapper

from dvc.exceptions import DvcException
from dvc.fs._callback import DEFAULT_CALLBACK, FsspecCallback
from dvc.ui import ui
from dvc.utils import tmp_fname
from dvc.utils.fs import makedirs, move

logger = logging.getLogger(__name__)


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
    TRAVERSE_PREFIX_LEN = 3
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

    @property
    def config(self):
        return self._config

    @cached_property
    def path(self):
        from .path import Path

        return Path(self.sep)

    @classmethod
    def _strip_protocol(cls, path: str):
        return path

    def unstrip_protocol(self, path):
        return path

    @staticmethod
    def _get_kwargs_from_urls(urlpath):  # pylint:disable=unused-argument
        return {}

    @classmethod
    def get_missing_deps(cls):
        import importlib

        missing = []
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

    def checksum(self, path) -> str:
        raise NotImplementedError

    def open(self, path, mode: str = "r", encoding: str = None, **kwargs):
        raise RemoteActionNotImplemented("open", self.scheme)

    def exists(self, path) -> bool:
        raise NotImplementedError

    # pylint: disable=unused-argument

    def isdir(self, path):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return False

    def isfile(self, path):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return True

    def iscopy(self, path):
        """Check if this file is an independent copy."""
        return False  # We can't be sure by default

    def walk(self, top, topdown=True, **kwargs):
        """Return a generator with (root, dirs, files)."""
        raise NotImplementedError

    def find(self, path, prefix=None):
        """Return a generator with `str`s to all the files.

        Optional kwargs:
            prefix (bool): If true `path` will be treated as a prefix
                rather than directory path.
        """
        raise NotImplementedError

    def is_empty(self, path):
        return False

    def info(self, path):
        raise NotImplementedError

    def getsize(self, path):
        return self.info(path).get("size")

    def remove(self, path):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def makedirs(self, path, **kwargs):
        """Optional: Implement only if the remote needs to create
        directories before copying/linking/moving data
        """

    def move(self, from_info, to_info):
        self.copy(from_info, to_info)
        self.remove(from_info)

    def copy(self, from_info, to_info):
        raise RemoteActionNotImplemented("copy", self.scheme)

    def symlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("symlink", self.scheme)

    def hardlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("hardlink", self.scheme)

    def reflink(self, from_info, to_info):
        raise RemoteActionNotImplemented("reflink", self.scheme)

    # pylint: enable=unused-argument

    def upload(
        self,
        from_info,
        to_info,
        total=None,
        desc=None,
        callback=None,
        no_progress_bar=False,
        **pbar_args,
    ):
        is_file_obj = hasattr(from_info, "read")
        method = "upload_fobj" if is_file_obj else "put_file"
        if not hasattr(self, method):
            raise RemoteActionNotImplemented(method, self.scheme)

        if not is_file_obj:
            from .local import localfs

            desc = desc or localfs.path.name(from_info)

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
            if total:
                callback.set_size(total)

        with stack:
            if is_file_obj:
                wrapped = CallbackIOWrapper(
                    callback.relative_update, from_info
                )
                # `size` is used to provide hints to the WebdavFileSystem
                # for legacy servers.
                # pylint: disable=no-member
                return self.upload_fobj(wrapped, to_info, size=total)

            logger.debug("Uploading '%s' to '%s'", from_info, to_info)
            # pylint: disable=no-member
            return self.put_file(
                os.fspath(from_info), to_info, callback=callback
            )

    def download(
        self,
        from_info,
        to_info,
        name=None,
        callback=None,
        no_progress_bar=False,
        jobs=None,
        _only_file=False,
        **kwargs,
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
        from_info,
        to_info,
        callback=DEFAULT_CALLBACK,
        jobs=None,
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

        download_files = FsspecCallback.wrap_fn(callback, self._download_file)
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
        from_info,
        to_info,
        callback=DEFAULT_CALLBACK,
    ):
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
