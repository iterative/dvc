import errno
import os

from funcy import first

from dvc.dvcfile import is_valid_filename
from dvc.exceptions import OutputNotFoundError
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.scm.tree import BaseTree
from dvc.utils.fs import copy_obj_to_file


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
            yield root.fspath, list(dirs), files

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

    def open(self, *args, **kwargs):
        if self.dvctree:
            try:
                return self.dvctree.open(*args, **kwargs)
            except FileNotFoundError:
                pass

        return self.repo.tree.open(*args, **kwargs)

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

    def _walk(self, top, topdown=True):
        if self.dvctree and not self.repo.tree.isdir(top):
            yield from self.dvctree.walk(top, topdown=topdown)
            return
        if not self.dvctree or not self.dvctree.isdir(top):
            yield from self.repo.tree.walk(top, topdown=topdown)
            return

        # walk and merge both trees, ensure that dvcfiles are ignored (handled
        # as DVC outs, not as git versioned files)
        repo_root, repo_dirs, repo_files = first(
            self.repo.tree.walk(top, topdown=topdown)
        )
        dvc_root, dvc_dirs, dvc_files = first(
            self.dvctree.walk(top, topdown=topdown)
        )
        dirs = list(set(repo_dirs) | set(dvc_dirs))
        files = set(dvc_files)
        for filename in repo_files:
            if is_valid_filename(filename):
                name, _ = os.path.splitext(filename)
                if not self.dvctree.exists(os.path.join(repo_root, name)):
                    files.add(filename)
            else:
                files.add(filename)
        yield repo_root, dirs, list(files)

        for dirname in dirs:
            yield from self._walk(
                os.path.join(repo_root, dirname), topdown=topdown
            )

    def walk(self, top, topdown=True):
        assert topdown

        if not self.exists(top):
            raise FileNotFoundError

        if not self.isdir(top):
            raise NotADirectoryError

        yield from self._walk(top, topdown=topdown)

    def copyfile(self, src, dest):
        """Copy specified file from this tree to the destination path."""
        if not self.isfile(src):
            raise FileNotFoundError

        with self.open(src, mode="rb", encoding=None) as fobj:
            copy_obj_to_file(fobj, dest)
