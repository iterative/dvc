import os
import shutil
from functools import lru_cache

from dvc.progress import Tqdm

from .base import BaseFileSystem


# pylint: disable=no-member
class FSSpecWrapper(BaseFileSystem):
    def __init__(self, repo, config):
        super().__init__(repo, config)
        self.fs_args = {"skip_instance_cache": True}
        self.fs_args.update(self._prepare_credentials(config))

    def fs(self):
        raise NotImplementedError

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

    def _strip_buckets(self, entries, detail, prefix=None):
        for entry in entries:
            if detail:
                entry = self._entry_hook(entry.copy())
                entry["name"] = self._strip_bucket(entry["name"])
            else:
                entry = self._strip_bucket(
                    f"{prefix}/{entry}" if prefix else entry
                )
            yield entry

    def _entry_hook(self, entry):
        """Simple hook method to be overriden when wanted to process
        entries within info() and ls(detail=True) calls"""
        return entry

    def _prepare_credentials(self, config):  # pylint: disable=unused-argument
        """Prepare the arguments for authentication to the
        host filesystem"""
        return {}

    def isdir(self, path_info):
        if not self.exists(path_info):
            return False

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

    def isfile(self, path_info):
        if not self.exists(path_info):
            return False

        return not self.isdir(path_info)

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
        self.fs.copy(self._with_bucket(from_info), self._with_bucket(to_info))

    def exists(self, path_info, use_dvcignore=False):
        return self.fs.exists(self._with_bucket(path_info))

    def ls(
        self, path_info, detail=False, recursive=False
    ):  # pylint: disable=arguments-differ
        if self.isdir(path_info) and self.is_empty(path_info):
            return None

        path = self._with_bucket(path_info)
        if recursive:
            for root, _, files in self.fs.walk(path, detail=detail):
                if detail:
                    files = files.values()
                yield from self._strip_buckets(files, detail, prefix=root)
            return None

        yield from self._strip_buckets(self.ls(path, detail=detail), detail)

    def walk_files(self, path_info, **kwargs):
        for file in self.ls(path_info, recursive=True):
            yield path_info.replace(path=file)

    def remove(self, path_info):
        self.fs.rm(self._with_bucket(path_info))

    def info(self, path_info):
        info = self.fs.info(self._with_bucket(path_info)).copy()
        info = self._entry_hook(info)
        info["name"] = self._strip_bucket(info["name"])
        return info

    def _upload_fobj(self, fobj, to_info):
        with self.open(to_info, "wb") as fdest:
            shutil.copyfileobj(fobj, fdest, length=fdest.blocksize)

    def _upload(
        self, from_file, to_info, name=None, no_progress_bar=False, **kwargs
    ):
        total = os.path.getsize(from_file)
        with open(from_file, "rb") as fobj:
            self.upload_fobj(
                fobj,
                self._with_bucket(to_info),
                desc=name,
                total=total,
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
