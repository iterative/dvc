import os
import shutil
from functools import lru_cache

from funcy import cached_property

from dvc.progress import Tqdm

from .base import BaseFileSystem
from .local import LocalFileSystem


# pylint: disable=no-member
class FSSpecWrapper(BaseFileSystem):
    TRAVERSE_PREFIX_LEN = 2

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.fs_args = {"skip_instance_cache": True}
        self.fs_args.update(self._prepare_credentials(**kwargs))

    @cached_property
    def fs(self):
        raise NotImplementedError

    def _with_bucket(self, path):
        return str(path)

    def _strip_bucket(self, entry):
        return entry

    @staticmethod
    def _get_kwargs_from_urls(urlpath):
        from fsspec.utils import infer_storage_options

        options = infer_storage_options(urlpath)
        options.pop("path", None)
        options.pop("protocol", None)
        return options

    def _strip_buckets(self, entries, detail=False):
        for entry in entries:
            if detail:
                entry = self._entry_hook(entry.copy())
                entry["name"] = self._strip_bucket(entry["name"])
            else:
                entry = self._strip_bucket(entry)
            yield entry

    def _entry_hook(self, entry):
        """Simple hook method to be overridden when wanted to process
        entries within info() and ls(detail=True) calls"""
        return entry

    def _prepare_credentials(
        self, **config
    ):  # pylint: disable=unused-argument
        """Prepare the arguments for authentication to the
        host filesystem"""
        return {}

    def _isdir(self, path_info):
        return self.fs.isdir(self._with_bucket(path_info))

    def isdir(self, path_info):
        try:
            return self._isdir(path_info)
        except FileNotFoundError:
            return False

    def isfile(self, path_info):
        try:
            return not self._isdir(path_info)
        except FileNotFoundError:
            return False

    def is_empty(self, path_info):
        entry = self.info(path_info)
        if entry["type"] == "directory":
            return not self.fs.ls(self._with_bucket(path_info))
        return entry["size"] == 0

    def open(
        self, path_info, mode="r", **kwargs
    ):  # pylint: disable=arguments-differ
        return self.fs.open(self._with_bucket(path_info), mode=mode)

    def copy(self, from_info, to_info):
        self.makedirs(to_info.parent)
        self.fs.copy(self._with_bucket(from_info), self._with_bucket(to_info))

    def exists(self, path_info) -> bool:
        return self.fs.exists(self._with_bucket(path_info))

    def ls(self, path_info, detail=False):
        path = self._with_bucket(path_info)
        files = self.fs.ls(path, detail=detail)
        yield from self._strip_buckets(files, detail=detail)

    # pylint: disable=unused-argument
    def find(self, path_info, detail=False, prefix=None):
        path = self._with_bucket(path_info)
        files = self.fs.find(path, detail=detail)
        if detail:
            files = files.values()

        yield from self._strip_buckets(files, detail=detail)

    def walk_files(self, path_info, **kwargs):
        for file in self.find(path_info, **kwargs):
            yield path_info.replace(path=file)

    def move(self, from_info, to_info):
        self.fs.move(self._with_bucket(from_info), self._with_bucket(to_info))

    def remove(self, path_info):
        self.fs.rm_file(self._with_bucket(path_info))

    def info(self, path_info):
        info = self.fs.info(self._with_bucket(path_info)).copy()
        info = self._entry_hook(info)
        info["name"] = self._strip_bucket(info["name"])
        return info

    def makedirs(self, path_info, **kwargs):
        self.fs.makedirs(
            self._with_bucket(path_info), exist_ok=kwargs.pop("exist_ok", True)
        )

    def _upload_fobj(self, fobj, to_info, size=None):
        self.makedirs(to_info.parent)
        with self.open(to_info, "wb") as fdest:
            shutil.copyfileobj(fobj, fdest, length=fdest.blocksize)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **kwargs
    ):
        self.makedirs(to_info.parent)
        size = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                to_info,
                size=size,
                desc=name,
                no_progress_bar=no_progress_bar,
            )
        self.fs.invalidate_cache(self._with_bucket(to_info.parent))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **pbar_args
    ):
        total = self.getsize(self._with_bucket(from_info))
        with self.open(from_info, "rb") as fobj:
            with Tqdm.wrapattr(
                fobj,
                "read",
                desc=name,
                disable=no_progress_bar,
                bytes=True,
                total=total,
                **pbar_args,
            ) as wrapped:
                with open(to_file, "wb") as fdest:
                    shutil.copyfileobj(wrapped, fdest, length=fobj.blocksize)


# pylint: disable=abstract-method
class ObjectFSWrapper(FSSpecWrapper):
    TRAVERSE_PREFIX_LEN = 3

    @lru_cache(512)
    def _with_bucket(self, path):
        if isinstance(path, self.PATH_CLS):
            return f"{path.bucket}/{path.path}"
        return path

    def _strip_bucket(self, entry):
        try:
            bucket, path = entry.split("/", 1)
        except ValueError:
            # If there is no path attached, only returns
            # the bucket (top-level).
            bucket, path = entry, None
        return path or bucket

    def makedirs(self, path_info, **kwargs):
        # For object storages make this method a no-op. The original
        # fs.makedirs() method will only check if the bucket exists
        # and create if it doesn't though we don't want to support
        # that behavior, and the check will cost some time so we'll
        # simply ignore all mkdir()/makedirs() calls.
        return None

    def _isdir(self, path_info):
        # Directory in object storages are interpreted differently
        # among different fsspec providers, so this logic is a temporary
        # measure for us to adapt as of now. It checks whether it is a
        # directory (as in a prefix with contents) or whether it is an empty
        # file where it's name ends with a forward slash

        entry = self.info(path_info)
        return entry["type"] == "directory" or (
            entry["size"] == 0
            and entry["type"] == "file"
            and entry["name"].endswith("/")
        )

    def find(self, path_info, detail=False, prefix=None):
        if prefix:
            path = self._with_bucket(path_info.parent)
            files = self.fs.find(
                path, detail=detail, prefix=path_info.parts[-1]
            )
        else:
            path = self._with_bucket(path_info)
            files = self.fs.find(path, detail=detail)

        if detail:
            files = files.values()

        # When calling find() on a file, it returns the same file in a list.
        # For object-based storages, the same behavior applies to empty
        # directories since they are represented as files. This condition
        # checks whether we should yield an empty list (if it is an empty
        # directory) or just yield the file itself.
        if len(files) == 1 and files[0] == path and self.isdir(path_info):
            return None

        yield from self._strip_buckets(files, detail=detail)


_LOCAL_FS = LocalFileSystem()


class CallbackMixin:
    """Use the native ``get_file()``/``put_file()`` APIs
    if the target filesystem supports callbacks."""

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **pbar_args
    ):
        with Tqdm(
            desc=name,
            disable=no_progress_bar,
            bytes=True,
            total=-1,
            **pbar_args,
        ) as pbar:
            self.fs.put_file(
                os.fspath(from_file),
                self._with_bucket(to_info),
                callback=pbar.as_callback(_LOCAL_FS, from_file),
            )
        self.fs.invalidate_cache(self._with_bucket(to_info.parent))

    def _download(
        self, from_info, to_file, name=None, no_progress_bar=False, **pbar_args
    ):
        with Tqdm(
            desc=name,
            disable=no_progress_bar,
            bytes=True,
            total=-1,
            **pbar_args,
        ) as pbar:
            self.fs.get_file(
                self._with_bucket(from_info),
                os.fspath(to_file),
                callback=pbar.as_callback(self, from_info),
            )
