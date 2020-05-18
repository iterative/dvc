import errno
import logging
import os

from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.remote.base import RemoteActionNotImplemented
from dvc.repo import Repo
from dvc.scm.tree import BaseTree, WorkingTree

logger = logging.getLogger(__name__)


class DvcTree(BaseTree):
    """DVC repo tree.

    Args:
        repo: DVC repo.
        fetch: if True, uncached DVC outs will be fetched on `open()`.
        stream: if True, uncached DVC outs will be streamed directly from
            remote on `open()`.

    `stream` takes precedence over `fetch`. If `stream` is enabled and
    a remote does not support streaming, uncached DVC outs will be fetched
    as a fallback.
    """

    def __init__(self, repo: Repo, fetch=False, stream=False):
        self.repo = repo
        self.fetch = fetch
        self.stream = stream

    def _find_outs(self, path, *args, **kwargs):
        outs = self.repo.find_outs_by_path(path, *args, **kwargs)

        def _is_cached(out):
            return out.use_cache

        outs = list(filter(_is_cached, outs))
        if not outs:
            raise OutputNotFoundError(path, self.repo)

        return outs

    def open(self, path, mode="r", encoding="utf-8", remote=None):
        try:
            outs = self._find_outs(path, strict=False)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        if len(outs) != 1 or outs[0].is_dir_checksum:
            raise OSError(errno.EISDIR)

        out = outs[0]
        # temporary hack to make cache use WorkingTree and not GitTree, because
        # cache dir doesn't exist in the latter.
        saved_tree = self.repo.tree
        self.repo.tree = WorkingTree(self.repo.root_dir)
        try:
            if out.changed_cache():
                if not self.fetch and not self.stream:
                    raise FileNotFoundError

                remote_obj = self.repo.cloud.get_remote(remote)
                if self.stream:
                    try:
                        remote_info = remote_obj.checksum_to_path_info(
                            out.checksum
                        )
                        return remote_obj.open(
                            remote_info, mode=mode, encoding=encoding
                        )
                    except RemoteActionNotImplemented:
                        pass
                cache_info = out.get_used_cache(remote=remote)
                self.repo.cloud.pull(cache_info, remote=remote)
        finally:
            self.repo.tree = saved_tree

        return open(out.cache_path, mode=mode, encoding=encoding)

    def exists(self, path):
        try:
            self._find_outs(path, strict=False, recursive=True)
            return True
        except OutputNotFoundError:
            return False

    def isdir(self, path):
        if not self.exists(path):
            return False

        path_info = PathInfo(os.path.abspath(path))
        outs = self._find_outs(path, strict=False, recursive=True)
        if len(outs) != 1 or outs[0].path_info != path_info:
            return True

        return outs[0].is_dir_checksum

    def isfile(self, path):
        if not self.exists(path):
            return False

        return not self.isdir(path)

    def _walk(self, root, trie, topdown=True):
        dirs = set()
        files = []

        root_len = len(root.parts)
        for key, out in trie.iteritems(prefix=root.parts):
            if key == root.parts:
                continue

            name = key[root_len]
            if len(key) > root_len + 1 or (out and out.is_dir_checksum):
                dirs.add(name)
                continue

            files.append(name)

        if topdown:
            dirs = list(dirs)
            yield root.fspath, dirs, files

            for dname in dirs:
                yield from self._walk(root / dname, trie)
        else:
            assert False

    def walk(self, top, topdown=True):
        from pygtrie import Trie

        assert topdown

        if not self.exists(top):
            raise FileNotFoundError

        if not self.isdir(top):
            raise NotADirectoryError

        root = PathInfo(os.path.abspath(top))
        outs = self._find_outs(top, recursive=True, strict=False)

        trie = Trie()

        for out in outs:
            trie[out.path_info.parts] = out

            if out.is_dir_checksum and (self.fetch or self.stream):
                # will pull dir cache if needed
                cache = out.collect_used_dir_cache()
                for _, names in cache.scheme_names(out.scheme):
                    for name in names:
                        path_info = out.path_info.parent / name
                        trie[path_info.parts] = None

        yield from self._walk(root, trie, topdown=topdown)

    def isdvc(self, path):
        try:
            return len(self._find_outs(path)) == 1
        except OutputNotFoundError:
            pass
        return False

    def isexec(self, path):
        return False


class RepoTree(BaseTree):
    def __init__(self, repo):
        self.repo = repo
        self.dvctree = DvcTree(repo)

    def open(self, *args, **kwargs):
        try:
            return self.dvctree.open(*args, **kwargs)
        except FileNotFoundError:
            pass

        return self.repo.tree.open(*args, **kwargs)

    def exists(self, path):
        return self.repo.tree.exists(path) or self.dvctree.exists(path)
