import errno
import os
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    Callable,
    Dict,
    Optional,
    Tuple,
)

from fsspec.spec import AbstractFileSystem

if TYPE_CHECKING:
    from io import BytesIO

    from dvc.scm.git import Git
    from dvc.scm.git.objects import GitTrie


def bytesio_len(obj: "BytesIO") -> Optional[int]:
    try:
        offset = obj.tell()
        length = obj.seek(0, os.SEEK_END)
        obj.seek(offset)
    except (AttributeError, OSError):
        return None
    return length


class GitFileSystem(AbstractFileSystem):
    # pylint: disable=abstract-method
    sep = os.sep
    cachable = False

    def __init__(
        self,
        path: str = None,
        rev: str = None,
        scm: "Git" = None,
        trie: "GitTrie" = None,
        rev_resolver: Callable[["Git", str], str] = None,
        **kwargs,
    ):
        from dvc.scm.git import Git
        from dvc.scm.git.objects import GitTrie

        super().__init__(**kwargs)
        if not trie:
            scm = scm or Git(path)
            resolver = rev_resolver or Git.resolve_rev
            resolved = resolver(scm, rev or "HEAD")
            tree_obj = scm.pygit2.get_tree_obj(rev=resolved)
            trie = GitTrie(tree_obj, resolved)
            path = scm.root_dir
        else:
            assert path

        self.trie = trie
        self.root_dir = path
        self.rev = self.trie.rev

    def _get_key(self, path: str) -> Tuple[str, ...]:
        from dvc.scm.utils import relpath

        if os.path.isabs(path):
            path = relpath(path, self.root_dir)
        relparts = path.split(os.sep)
        if relparts == ["."]:
            return ()
        return tuple(relparts)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int = None,
        autocommit: bool = True,
        cache_options: Dict = None,
        **kwargs: Any,
    ) -> BinaryIO:
        key = self._get_key(path)
        try:
            obj = self.trie.open(key, mode=mode)
            obj.size = bytesio_len(obj)
            return obj
        except KeyError as exc:
            msg = os.strerror(errno.ENOENT) + f"in branch '{self.rev}'"
            raise FileNotFoundError(errno.ENOENT, msg, path) from exc
        except IsADirectoryError as exc:
            raise IsADirectoryError(
                errno.EISDIR, os.strerror(errno.EISDIR), path
            ) from exc

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        key = self._get_key(path)
        try:
            return {
                **self.trie.info(key),
                "name": os.path.join(self.root_dir, self.sep.join(key)),
            }
        except KeyError:
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), path
            )

    def exists(self, path: str, **kwargs: Any) -> bool:
        key = self._get_key(path)
        return self.trie.exists(key)

    def checksum(self, path: str) -> str:
        return self.info(path)["sha"]

    def walk(  # pylint: disable=arguments-differ
        self,
        top: str,
        topdown: bool = True,
        onerror: Callable[[OSError], None] = None,
        maxdepth: int = None,
        detail: bool = False,
        **kwargs: Any,
    ):
        """Directory tree generator.

        See `os.walk` for the docs. Differences:
        - no support for symlinks
        """
        assert maxdepth is None  # not supported yet.
        if not self.isdir(top):
            if onerror:
                if self.exists(top):
                    exc: OSError = NotADirectoryError(
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
            root = self.root_dir

            if prefix:
                root = os.path.join(root, os.sep.join(prefix))
            if detail:
                yield (
                    root,
                    {d: self.info(os.path.join(root, d)) for d in dirs},
                    {f: self.info(os.path.join(root, f)) for f in files},
                )
            else:
                yield root, dirs, files

    def ls(self, path, detail=True, **kwargs):
        for _, dirs, files in self.walk(path, detail=detail, **kwargs):
            merge = files.update if detail else files.extend
            merge(dirs)
            return files
