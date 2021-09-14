import errno
import os

from tqdm.utils import CallbackIOWrapper

from dvc.utils import is_exec, relpath

from ..progress import DEFAULT_CALLBACK
from .base import BaseFileSystem


class GitFileSystem(BaseFileSystem):  # pylint:disable=abstract-method
    """Proxies the repo file access methods to Git objects"""

    sep = os.sep

    scheme = "local"

    def __init__(self, root_dir, trie):
        super().__init__()
        self._root = root_dir
        self.trie = trie

    @property
    def rev(self):
        return self.trie.rev

    def _get_key(self, path):
        if isinstance(path, str):
            if not os.path.isabs(path):
                relparts = path.split(os.sep)
            else:
                relparts = relpath(path, self._root).split(os.sep)
        else:
            relparts = path.relative_to(self._root).parts
        if relparts == ["."]:
            return ()
        return tuple(relparts)

    def open(
        self, path, mode="r", encoding=None, **kwargs
    ):  # pylint: disable=arguments-renamed
        # NOTE: this is incorrect, we need to use locale to determine default
        # encoding.
        encoding = encoding or "utf-8"

        key = self._get_key(path)
        try:
            return self.trie.open(key, mode=mode, encoding=encoding)
        except KeyError as exc:
            msg = os.strerror(errno.ENOENT) + f"in branch '{self.rev}'"
            raise FileNotFoundError(errno.ENOENT, msg, path) from exc
        except IsADirectoryError as exc:
            raise IsADirectoryError(
                errno.EISDIR, os.strerror(errno.EISDIR), path
            ) from exc

    def exists(self, path_info) -> bool:
        key = self._get_key(path_info)
        return self.trie.exists(key)

    def isdir(self, path_info) -> bool:
        key = self._get_key(path_info)
        return self.trie.isdir(key)

    def isfile(self, path_info) -> bool:
        key = self._get_key(path_info)
        return self.trie.isfile(key)

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        if not self.isdir(top):
            if onerror:
                if self.exists(top):
                    exc = NotADirectoryError(
                        errno.ENOTDIR, os.strerror(errno.ENOTDIR), top
                    )
                else:
                    exc = FileNotFoundError(
                        errno.ENOENT, os.strerror(errno.ENOENT), top
                    )
                onerror(exc)
            return []

        key = self._get_key(top)
        for prefix, dirs, files in self.trie.walk(key, topdown=topdown):
            if prefix:
                root = os.path.join(self._root, os.sep.join(prefix))
            else:
                root = self._root
            yield root, dirs, files

    def isexec(self, path_info):
        if not self.exists(path_info):
            return False

        mode = self.info(path_info)["mode"]
        return is_exec(mode)

    def info(self, path_info):
        key = self._get_key(path_info)
        try:
            return self.trie.info(key)
        except KeyError:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path_info
            )

    def checksum(self, path_info):
        return self.info(path_info)["sha"]

    def walk_files(self, path_info, **kwargs):
        for root, _, files in self.walk(path_info, **kwargs):
            for file in files:
                # NOTE: os.path.join is ~5.5 times slower
                yield f"{root}{os.sep}{file}"

    def get_file(
        self, from_info, to_file, callback=DEFAULT_CALLBACK, **kwargs
    ):
        import shutil

        total = self.getsize(from_info)
        if total:
            callback.set_size(total)

        with self.open(from_info, "rb", **kwargs) as from_fobj:
            with open(to_file, "wb+") as to_fobj:
                wrapped = CallbackIOWrapper(
                    callback.relative_update, from_fobj
                )
                shutil.copyfileobj(wrapped, to_fobj)
