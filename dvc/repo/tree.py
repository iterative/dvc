import logging
import os
from typing import Optional

from funcy import cached_property, post_processing

from dvc.dvcfile import is_valid_filename
from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.tree.base import BaseTree, RemoteActionNotImplemented
from dvc.utils import file_md5
from dvc.utils.fs import copy_fobj_to_file, makedirs

logger = logging.getLogger(__name__)


class DvcTree(BaseTree):  # pylint:disable=abstract-method
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
        super().__init__(repo, {"url": repo.root_dir})
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
        assert isinstance(path, PathInfo)
        if not self.fetch and not self.stream:
            raise FileNotFoundError
        dir_cache = out.get_dir_cache(remote=remote)
        for entry in dir_cache:
            entry_relpath = entry[out.tree.PARAM_RELPATH]
            if os.name == "nt":
                entry_relpath = entry_relpath.replace("/", os.sep)
            if path == out.path_info / entry_relpath:
                return entry[out.tree.PARAM_CHECKSUM]
        raise FileNotFoundError

    def open(
        self, path, mode="r", encoding="utf-8", remote=None
    ):  # pylint: disable=arguments-differ
        try:
            outs = self._find_outs(path, strict=False)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        # NOTE: this handles both dirty and checkout-ed out at the same time
        if self.repo.tree.exists(path):
            return self.repo.tree.open(path, mode=mode, encoding=encoding)

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
                    remote_info = remote_obj.tree.hash_to_path_info(checksum)
                    return remote_obj.tree.open(
                        remote_info, mode=mode, encoding=encoding
                    )
                except RemoteActionNotImplemented:
                    pass
            cache_info = out.get_used_cache(filter_info=path, remote=remote)
            self.repo.cloud.pull(cache_info, remote=remote)

        if out.is_dir_checksum:
            checksum = self._get_granular_checksum(path, out)
            cache_path = out.cache.tree.hash_to_path_info(checksum).url
        else:
            cache_path = out.cache_path
        return open(cache_path, mode=mode, encoding=encoding)

    def exists(self, path):  # pylint: disable=arguments-differ
        try:
            self._find_outs(path, strict=False, recursive=True)
            return True
        except OutputNotFoundError:
            return False

    def isdir(self, path):  # pylint: disable=arguments-differ
        if not self.exists(path):
            return False

        path_info = PathInfo(os.path.abspath(path))
        outs = self._find_outs(path, strict=False, recursive=True)
        if len(outs) != 1:
            return True

        out = outs[0]
        if not out.is_dir_checksum:
            if out.path_info != path_info:
                return True
            return False

        if out.path_info == path_info:
            return True

        # for dir checksum, we need to check if this is a file inside the
        # directory
        try:
            self._get_granular_checksum(path_info, out)
            return False
        except FileNotFoundError:
            return True

    def isfile(self, path):  # pylint: disable=arguments-differ
        if not self.exists(path):
            return False

        return not self.isdir(path)

    def _add_dir(self, top, trie, out, download_callback=None, **kwargs):
        if not self.fetch and not self.stream:
            return

        # pull dir cache if needed
        dir_cache = out.get_dir_cache(**kwargs)

        # pull dir contents if needed
        if self.fetch:
            if out.changed_cache(filter_info=top):
                used_cache = out.get_used_cache(filter_info=top)
                downloaded = self.repo.cloud.pull(used_cache, **kwargs)
                if download_callback:
                    download_callback(downloaded)

        for entry in dir_cache:
            entry_relpath = entry[out.tree.PARAM_RELPATH]
            if os.name == "nt":
                entry_relpath = entry_relpath.replace("/", os.sep)
            path_info = out.path_info / entry_relpath
            trie[path_info.parts] = None

    def _walk(self, root, trie, topdown=True, **kwargs):
        dirs = set()
        files = []

        out = trie.get(root.parts)
        if out and out.is_dir_checksum:
            self._add_dir(root, trie, out, **kwargs)

        root_len = len(root.parts)
        for key, out in trie.iteritems(prefix=root.parts):  # noqa: B301
            if key == root.parts:
                continue

            name = key[root_len]
            if len(key) > root_len + 1 or (out and out.is_dir_checksum):
                dirs.add(name)
                continue

            files.append(name)

        assert topdown
        dirs = list(dirs)
        yield root.fspath, dirs, files

        for dname in dirs:
            yield from self._walk(root / dname, trie)

    def walk(self, top, topdown=True, onerror=None, **kwargs):
        from pygtrie import Trie

        assert topdown

        if not self.exists(top):
            if onerror is not None:
                onerror(FileNotFoundError(top))
            return

        if not self.isdir(top):
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        root = PathInfo(os.path.abspath(top))
        outs = self._find_outs(top, recursive=True, strict=False)

        trie = Trie()

        for out in outs:
            trie[out.path_info.parts] = out

            if out.is_dir_checksum and root.isin_or_eq(out.path_info):
                self._add_dir(top, trie, out, **kwargs)

        yield from self._walk(root, trie, topdown=topdown, **kwargs)

    def isdvc(self, path, **kwargs):
        try:
            return len(self._find_outs(path, **kwargs)) == 1
        except OutputNotFoundError:
            pass
        return False

    def isexec(self, path):  # pylint: disable=unused-argument
        return False

    def get_file_hash(self, path_info):
        outs = self._find_outs(path_info, strict=False)
        if len(outs) != 1:
            raise OutputNotFoundError
        out = outs[0]
        if out.is_dir_checksum:
            return self._get_granular_checksum(path_info, out)
        return out.checksum


class RepoTree(BaseTree):  # pylint:disable=abstract-method
    """DVC + git-tracked files tree.

    Args:
        repo: DVC or git repo.

    Any kwargs will be passed to `DvcTree()`.
    """

    def __init__(
        self, tree, subrepos=None, **kwargs
    ):  # pylint: disable=super-init-not-called
        logger.info("original subrepos: %s", subrepos)
        subrepos = subrepos or []
        subrepos.sort(key=lambda r: len(r.root_dir), reverse=True)
        logger.info("sorted subrepos: %r", subrepos)
        dvctrees = [
            (os.path.abspath(repo.root_dir), DvcTree(repo, **kwargs))
            for repo in subrepos
        ]
        logger.info("dvctrees tuples: %r", dvctrees)
        self._dvctrees = dict(
            sorted(dvctrees, key=lambda v: len(v[0]), reverse=True)
        )
        logger.info("dvctrees: %r", self._dvctrees)
        self.tree = tree
        self._kwargs = kwargs

    @property
    def fetch(self):
        return self._kwargs.get("fetch", False)

    @property
    def stream(self):
        return self._kwargs.get("stream", False)

    def _find_subtree(self, path_prefix) -> Optional[DvcTree]:
        return self._find_subtree_with_prefix(path_prefix)[1]

    def _find_subtree_with_prefix(self, path):
        # dvctrees is already ordered from low to high
        path_prefix = os.path.abspath(path)
        logger.info("Path prefix: %s, original: %s", path_prefix, path)
        for pref, tree in self._dvctrees.items():
            if path_prefix.startswith(pref):
                return pref, tree
        return "", None

    def open(
        self, path, mode="r", encoding="utf-8", **kwargs
    ):  # pylint: disable=arguments-differ
        if "b" in mode:
            encoding = None

        subtree = self._find_subtree(path)
        if subtree and subtree.exists(path):
            return subtree.open(path, mode=mode, encoding=encoding, **kwargs)
        return self.tree.open(path, mode=mode, encoding=encoding)

    def open_by_relpath(self, path, *args, **kwargs):
        return self.open(PathInfo(self.tree.tree_root) / path, *args, **kwargs)

    def exists(self, path):  # pylint: disable=arguments-differ
        subtree = self._find_subtree(path)
        return self.tree.exists(path) or (subtree and subtree.exists(path))

    def isdir(self, path):  # pylint: disable=arguments-differ
        subtree = self._find_subtree(path)
        return self.tree.isdir(path) or (subtree and subtree.isdir(path))

    def isdvc(self, path, **kwargs):
        subtree = self._find_subtree(path)
        return subtree is not None and subtree.isdvc(path, **kwargs)

    def isfile(self, path):  # pylint: disable=arguments-differ
        subtree = self._find_subtree(path)
        return self.tree.isfile(path) or (subtree and subtree.isfile(path))

    def isexec(self, path):
        subtree = self._find_subtree(path)
        if subtree and subtree.exists(path):
            return subtree.isexec(path)
        return self.tree.isexec(path)

    def stat(self, path):
        return self.tree.stat(path)

    def _dvc_walk(self, walk):
        try:
            root, dirs, files = next(walk)
        except StopIteration:
            return
        yield root, dirs, files
        for _ in dirs:
            yield from self._dvc_walk(walk)

    def _repo_walk(self, dir_path, walk, **kwargs):
        assert os.path.isabs(dir_path)
        subtree = self._dvctrees.get(dir_path)
        if subtree:
            dvc_walk = subtree.walk(dir_path, topdown=True)
            yield from self._walk(walk, dvc_walk, **kwargs)
        else:
            try:
                root, dirs, files = next(walk)
            except StopIteration:
                return
            yield root, dirs, files
            for dirname in dirs:
                yield from self._repo_walk(
                    os.path.join(root, dirname), walk, **kwargs
                )

    def _walk(self, repo_walk, dvc_walk=None, dvcfiles=False):
        assert repo_walk
        try:
            _, dvc_dirs, dvc_fnames = (
                next(dvc_walk) if dvc_walk else (None, [], [])
            )
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
                yield from self._walk(repo_walk, dvc_walk, dvcfiles=dvcfiles)
            elif dirname in dvc_set:
                yield from self._dvc_walk(dvc_walk)
            elif dirname in repo_set:
                yield from self._repo_walk(
                    os.path.join(repo_root, dirname),
                    repo_walk,
                    dvcfiles=dvcfiles,
                )

    def walk(
        self, top, topdown=True, onerror=None, dvcfiles=False, **kwargs
    ):  # pylint: disable=arguments-differ
        """Walk and merge both DVC and repo trees.

        Args:
            top: path to walk from
            topdown: if True, tree will be walked from top down.
            onerror: if set, onerror function will be called if an error
                occurs (by default errors are ignored).
            dvcfiles: if True, dvcfiles will be included in the files list
                for walked directories.

        Any kwargs will be passed into methods used for fetching and/or
        streaming DVC outs from remotes.
        """
        assert topdown

        if not self.exists(top):
            if onerror is not None:
                onerror(FileNotFoundError(top))
            return

        if not self.isdir(top):
            if onerror is not None:
                onerror(NotADirectoryError(top))
            return

        subtree = self._find_subtree(top)
        dvc_exists = subtree and subtree.exists(top)
        repo_exists = self.tree.exists(top)
        if dvc_exists and not repo_exists:
            yield from subtree.walk(
                top, topdown=topdown, onerror=onerror, **kwargs
            )
        if not dvc_exists and not repo_exists:
            repo_walk = self.tree.walk(top, topdown=topdown, onerror=onerror)
            yield from self._repo_walk(
                os.path.abspath(top), repo_walk, dvcfiles=dvcfiles
            )
        dvc_walk = (
            subtree.walk(top, topdown=topdown, **kwargs)
            if dvc_exists
            else None
        )
        repo_walk = self.tree.walk(top, topdown=topdown)
        yield from self._walk(repo_walk, dvc_walk, dvcfiles=dvcfiles)

    def walk_files(self, top, **kwargs):  # pylint: disable=arguments-differ
        for root, _, files in self.walk(top, **kwargs):
            for fname in files:
                yield PathInfo(root) / fname

    def get_file_hash(self, path_info):
        """Return file checksum for specified path.

        If path_info is a DVC out, the pre-computed checksum for the file
        will be used. If path_info is a git file, MD5 will be computed for
        the git object.
        """
        if not self.exists(path_info):
            raise FileNotFoundError
        subtree = self._find_subtree(path_info)
        subtree_exists = subtree is not None and subtree.exists(path_info)
        if subtree_exists:
            return subtree.get_file_hash(path_info)
        return file_md5(path_info, self)[0]

    def copytree(self, top, dest):
        top = PathInfo(top)
        dest = PathInfo(dest)

        if not self.exists(top):
            raise FileNotFoundError

        if self.isfile(top):
            makedirs(dest.parent, exist_ok=True)
            with self.open(top, mode="rb") as fobj:
                copy_fobj_to_file(fobj, dest)
            return

        for root, _, files in self.walk(top):
            root = PathInfo(root)
            dest_dir = dest / root.relative_to(top)
            makedirs(dest_dir, exist_ok=True)
            for fname in files:
                src = root / fname
                with self.open(src, mode="rb") as fobj:
                    copy_fobj_to_file(fobj, dest_dir / fname)

    @cached_property
    def hash_jobs(self):
        return self.tree.hash_jobs

    def in_subtree(self, path):
        return self._find_subtree(path)

    @post_processing("\r\n".join)
    def _visualize(self, top, **kwargs):
        """`tree`-like output, useful for debugging/visualizing"""
        indent = 4
        spacing = " " * indent
        tee = "├── "
        last = "└── "
        for root, _, files in self.walk(top, **kwargs):
            level = root.replace(top, "").count(os.sep)
            indent = spacing * level
            yield "{}{}/".format(indent, os.path.basename(root))
            sub_indent = spacing * (level + 1)
            length = len(files)
            for i, f in enumerate(files):
                yield "{}{}{}".format(
                    sub_indent, tee if i + 1 != length else last, f
                )
