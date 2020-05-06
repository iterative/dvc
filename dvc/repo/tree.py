import errno
import logging
import os

from dvc.dvcfile import is_valid_filename
from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.scm.tree import BaseTree
from dvc.utils.fs import copyfile, copy_obj_to_file, makedirs

logger = logging.getLogger(__name__)


class DvcTree(BaseTree):
    def __init__(self, repo):
        self.repo = repo

    def _find_outs(self, path, *args, **kwargs):
        outs = self.repo.find_outs_by_path(path, *args, **kwargs)

        def _is_cached(out):
            return out.use_cache

        outs = list(filter(_is_cached, outs))
        if not outs:
            raise OutputNotFoundError(path, self.repo)

        return outs

    def open(self, path, mode="r", encoding="utf-8"):
        try:
            outs = self._find_outs(path, strict=False)
        except OutputNotFoundError as exc:
            raise FileNotFoundError from exc

        if len(outs) != 1 or outs[0].is_dir_checksum:
            raise IOError(errno.EISDIR)

        out = outs[0]
        if out.changed_cache():
            raise FileNotFoundError

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
            if len(key) > root_len + 1 or out.is_dir_checksum:
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

        yield from self._walk(root, trie, topdown=topdown)

    def isdvc(self, path):
        try:
            return (
                len(self._find_outs(path, recursive=True, strict=False)) == 1
            )
        except OutputNotFoundError:
            pass
        return False

    def isexec(self, path):
        return False


class RepoTree(BaseTree):
    def __init__(self, repo):
        self.repo = repo
        if isinstance(repo, Repo):
            self.dvctree = DvcTree(repo)
        else:
            # git-only erepo's do not need dvctree
            self.dvctree = None

    def open(self, path, **kwargs):
        if self.dvctree and self.dvctree.exists(path):
            try:
                return self.dvctree.open(path, **kwargs)
            except FileNotFoundError:
                pass

        return self.repo.tree.open(path, **kwargs)

    def exists(self, path):
        return self.repo.tree.exists(path) or (
            self.dvctree and self.dvctree.exists(path)
        )

    def isdir(self, path):
        return self.repo.tree.isdir(path) or (
            self.dvctree and self.dvctree.isdir(path)
        )

    def isdvc(self, path):
        return self.dvctree and self.dvctree.isdvc(path)

    def isfile(self, path):
        return self.repo.tree.isfile(path) or (
            self.dvctree and self.dvctree.isfile(path)
        )

    def _walk(self, dvc_walk, repo_walk):
        """Walk and merge both trees.

        Ensure that dvcfiles are handled as DVC outs and not as git versioned
        files.
        """

        dvc_root, dvc_dirs, dvc_files = next(dvc_walk)
        repo_root, repo_dirs, repo_files = next(repo_walk)
        assert dvc_root == repo_root

        # separate subdirs into shared dirs, dvc-only dirs, repo-only dirs
        dvc_set = set(dvc_dirs)
        repo_set = set(repo_dirs)
        dvc_only = list(dvc_set - repo_set)
        repo_only = list(repo_set - dvc_set)
        shared = list(dvc_set & repo_set)
        dirs = shared + dvc_only + repo_only

        files = set(dvc_files)
        for filename in repo_files:
            if is_valid_filename(filename):
                name, _ = os.path.splitext(filename)
                if not self.dvctree.exists(os.path.join(repo_root, name)):
                    files.add(filename)
            else:
                files.add(filename)
        yield repo_root, dirs, list(files)

        dvc_dirs[:] = [dirname for dirname in dirs if dirname in dvc_set]
        repo_dirs[:] = [dirname for dirname in dirs if dirname in repo_set]
        if not repo_dirs:
            yield from dvc_walk
        elif not dvc_dirs:
            yield from repo_walk
        else:
            yield from self._walk_both(dvc_walk, repo_walk)

    def walk(self, top, topdown=True):
        assert topdown

        dvc_exists = self.dvctree and self.dvctree.exists(top)
        repo_exists = self.repo.tree.exists(top)
        if dvc_exists and not repo_exists:
            yield from self.dvctree.walk(top, topdown=topdown)
            return
        if repo_exists and not dvc_exists:
            yield from self.repo.tree.walk(top, topdown=topdown)
            return
        if not dvc_exists and not repo_exists:
            raise FileNotFoundError

        if not self.isdir(top):
            raise NotADirectoryError

        dvc_walk = self.dvctree.walk(top, topdown=topdown)
        repo_walk = self.repo.tree.walk(top, topdown=topdown)
        yield from self._walk_both(dvc_walk, repo_walk)

    def copyfile(self, src, dest):
        """Copy specified file from this tree to the destination path."""
        if not self.exists(src):
            raise FileNotFoundError

        with self.open(src, mode="rb", encoding=None) as fobj:
            copy_obj_to_file(fobj, dest)

    def copytree(self, top, dest):
        """Copy directory/file tree to dest, starting from top."""
        if not self.exists(top):
            raise FileNotFoundError

        top = PathInfo(top)
        dest = PathInfo(dest)

        if self.isfile(top):
            return self.copyfile(top, dest)

        if self.isdvc(top):
            return self._copytree_dvc(top, dest)

        for root, dirs, files in self.walk(top):
            root_path = PathInfo(root)
            dest_dir = dest / root_path.relative_to(top)
            if not os.path.exists(dest_dir):
                makedirs(dest_dir)
            for filename in files:
                self.copyfile(root_path / filename, dest_dir / filename)

    def _copytree_dvc(self, top, dest):
        """Copy contents of DVC dir out from local cache to dest.

        Only dir cache contents starting from top will be copied.
        """
        try:
            (out,) = self.repo.find_outs_by_path(top, strict=False)
        except OutputNotFoundError:
            raise FileNotFoundError

        filter_info = PathInfo(os.path.abspath(top))
        for checksum, entry_path in out.filter_dir_cache(filter_info):
            entry_info = self.repo.cache.local.checksum_to_path_info(checksum)
            entry_path = PathInfo(os.path.abspath(entry_path))
            if top == entry_path:
                if not os.path.exists(dest.parent):
                    makedirs(dest.parent)
                copyfile(entry_info, dest)
                return
            elif top.overlaps(entry_path):
                dest_path = dest / entry_path.relative_to(top)
                if not os.path.exists(dest_path.parent):
                    makedirs(dest_path.parent)
                copyfile(entry_info, dest_path)
