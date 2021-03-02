import os
import shutil

from dvc.progress import Tqdm

from .base import BaseFileSystem


# pylint: disable=no-member
class FSSpecWrapper(BaseFileSystem):
    def fs(self):
        raise NotImplementedError

    def _with_bucket(self, path):
        if isinstance(path, self.PATH_CLS):
            return f"{path.bucket}/{path.path}"
        return path

    def _strip_bucket(self, entry):
        _, entry = entry.split("/", 1)
        return entry

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

    def isdir(self, path_info):
        return self.fs.isdir(self._with_bucket(path_info))

    def isfile(self, path_info):
        return self.fs.isfile(self._with_bucket(path_info))

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
