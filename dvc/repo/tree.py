import logging
import os

from dvc.dvcfile import is_valid_filename
from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.remote.base import RemoteActionNotImplemented
from dvc.scm.tree import BaseTree
from dvc.utils import file_md5

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

    def __init__(self, repo, fetch=False, stream=False):
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

    def _get_granular_checksum(self, path, out, remote=None):
        if not self.fetch and not self.stream:
            raise FileNotFoundError
        dir_cache = out.get_dir_cache(remote=remote)
        for entry in dir_cache:
            if path == out.path_info / entry[out.remote.PARAM_RELPATH]:
                return entry[out.remote.PARAM_CHECKSUM]
        raise FileNotFoundError

    def open(self, path, mode="r", encoding="utf-8", remote=None):
        try:
            outs = self._find_outs(path, strict=False)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        if len(outs) != 1 or (
            outs[0].is_dir_checksum and path == outs[0].path_info
        ):
            raise IsADirectoryError

        out = outs[0]
        if out.changed_cache(filter_info=path):
            if not self.fetch and not self.stream:
                raise FileNotFoundError

            remote_obj = self.repo.cloud.get_remote(remote)
            if self.stream:
                if out.is_dir_checksum:
                    checksum = self._get_granular_checksum(path, out)
                else:
                    checksum = out.checksum
                try:
                    remote_info = remote_obj.checksum_to_path_info(checksum)
                    return remote_obj.open(
                        remote_info, mode=mode, encoding=encoding
                    )
                except RemoteActionNotImplemented:
                    pass
                cache_info = out.get_used_cache(
                    filter_info=path, remote=remote
                )
                self.repo.cloud.pull(cache_info, remote=remote)

        if out.is_dir_checksum:
            checksum = self._get_granular_checksum(path, out)
            cache_path = out.cache.checksum_to_path_info(checksum).url
        else:
            cache_path = out.cache_path
        return open(cache_path, mode=mode, encoding=encoding)

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

    def walk(self, top, topdown=True, **kwargs):
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
                # pull dir cache if needed
                dir_cache = out.get_dir_cache(**kwargs)

                # pull dir contents if needed
                if self.fetch:
                    if out.changed_cache(filter_info=top):
                        used_cache = out.get_used_cache(filter_info=top)
                        self.repo.cloud.pull(used_cache, **kwargs)

                for entry in dir_cache:
                    entry_relpath = entry[out.remote.PARAM_RELPATH]
                    path_info = out.path_info / entry_relpath
                    trie[path_info.parts] = None

        yield from self._walk(root, trie, topdown=topdown)

    def isdvc(self, path, **kwargs):
        try:
            return len(self._find_outs(path, **kwargs)) == 1
        except OutputNotFoundError:
            pass
        return False

    def isexec(self, path):
        return False

    def get_file_checksum(self, path_info):
        outs = self._find_outs(path_info, strict=False)
        if len(outs) != 1:
            raise OutputNotFoundError
        out = outs[0]
        if out.is_dir_checksum:
            return self._get_granular_checksum(path_info, out)
        return out.checksum


class RepoTree(BaseTree):
    """DVC + git-tracked files tree.

    Args:
        repo: DVC or git repo.

    Any kwargs will be passed to `DvcTree()`.
    """

    def __init__(self, repo, **kwargs):
        self.repo = repo
        if hasattr(repo, "dvc_dir"):
            self.dvctree = DvcTree(repo, **kwargs)
        else:
            # git-only erepo's do not need dvctree
            self.dvctree = None

    def open(self, path, mode="r", encoding="utf-8", **kwargs):
        if "b" in mode:
            encoding = None

        if self.dvctree and self.dvctree.exists(path):
            try:
                return self.dvctree.open(
                    path, mode=mode, encoding=encoding, **kwargs
                )
            except FileNotFoundError:
                if self.isdvc(path):
                    raise
        return self.repo.tree.open(path, mode=mode, encoding=encoding)

    def exists(self, path):
        return self.repo.tree.exists(path) or (
            self.dvctree and self.dvctree.exists(path)
        )

    def isdir(self, path):
        return self.repo.tree.isdir(path) or (
            self.dvctree and self.dvctree.isdir(path)
        )

    def isdvc(self, path, **kwargs):
        return self.dvctree is not None and self.dvctree.isdvc(path, **kwargs)

    def isfile(self, path):
        return self.repo.tree.isfile(path) or (
            self.dvctree and self.dvctree.isfile(path)
        )

    def isexec(self, path):
        if self.dvctree and self.dvctree.exists(path):
            return self.dvctree.isexec(path)
        return self.repo.tree.isexec(path)

    def stat(self, path):
        return self.repo.tree.stat(path)

    def _walk_one(self, walk):
        try:
            root, dirs, files = next(walk)
        except StopIteration:
            return
        yield root, dirs, files
        for _ in dirs:
            yield from self._walk_one(walk)

    def _walk(self, dvc_walk, repo_walk, dvcfiles=False):
        try:
            _, dvc_dirs, dvc_fnames = next(dvc_walk)
            repo_root, repo_dirs, repo_fnames = next(repo_walk)
        except StopIteration:
            return

        # separate subdirs into shared dirs, dvc-only dirs, repo-only dirs
        dvc_set = set(dvc_dirs)
        repo_set = set(repo_dirs)
        dvc_only = list(dvc_set - repo_set)
        repo_only = list(repo_set - dvc_set)
        shared = list(dvc_set & repo_set)
        dirs = shared + dvc_only + repo_only

        # merge file lists
        files = {
            fname
            for fname in dvc_fnames + repo_fnames
            if dvcfiles or not is_valid_filename(fname)
        }

        yield repo_root, dirs, list(files)

        # set dir order for next recursion level - shared dirs first so that
        # next() for both generators recurses into the same shared directory
        dvc_dirs[:] = [dirname for dirname in dirs if dirname in dvc_set]
        repo_dirs[:] = [dirname for dirname in dirs if dirname in repo_set]

        for dirname in dirs:
            if dirname in shared:
                yield from self._walk(dvc_walk, repo_walk, dvcfiles=dvcfiles)
            elif dirname in dvc_set:
                yield from self._walk_one(dvc_walk)
            elif dirname in repo_set:
                yield from self._walk_one(repo_walk)

    def walk(self, top, topdown=True, dvcfiles=False, **kwargs):
        """Walk and merge both DVC and repo trees.

        Args:
            top: path to walk from
            topdown: if True, tree will be walked from top down.
            dvcfiles: if True, dvcfiles will be included in the files list
                for walked directories.

        Any kwargs will be passed into methods used for fetching and/or
        streaming DVC outs from remotes.
        """
        assert topdown

        if not self.exists(top):
            raise FileNotFoundError

        if not self.isdir(top):
            raise NotADirectoryError

        dvc_exists = self.dvctree and self.dvctree.exists(top)
        repo_exists = self.repo.tree.exists(top)
        if dvc_exists and not repo_exists:
            yield from self.dvctree.walk(top, topdown=topdown, **kwargs)
            return
        if repo_exists and not dvc_exists:
            yield from self.repo.tree.walk(top, topdown=topdown)
            return
        if not dvc_exists and not repo_exists:
            raise FileNotFoundError

        dvc_walk = self.dvctree.walk(top, topdown=topdown, **kwargs)
        repo_walk = self.repo.tree.walk(top, topdown=topdown)
        yield from self._walk(dvc_walk, repo_walk, dvcfiles=dvcfiles)

    def walk_files(self, top, **kwargs):
        for root, _, files in self.walk(top, **kwargs):
            for fname in files:
                yield PathInfo(root) / fname

    def get_file_checksum(self, path_info):
        """Return file checksum for specified path.

        If path_info is a DVC out, the pre-computed checksum for the file
        will be used. If path_info is a git file, MD5 will be computed for
        the git object.
        """
        if not self.exists(path_info):
            raise FileNotFoundError
        if self.dvctree and self.dvctree.exists(path_info):
            try:
                return self.dvctree.get_file_checksum(path_info)
            except OutputNotFoundError:
                pass
        return file_md5(path_info, self)[0]
