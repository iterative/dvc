import errno
import logging
import os

from dvc.dvcfile import is_valid_filename
from dvc.exceptions import (
    CheckoutError,
    DownloadError,
    OutputNotFoundError,
    RecursiveImportError,
)
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.scm.tree import BaseTree
from dvc.utils import file_md5, tmp_fname
from dvc.utils.fs import copy_obj_to_file, makedirs, move, remove

logger = logging.getLogger(__name__)


class DvcTree(BaseTree):
    def __init__(self, repo):
        self.repo = repo

    def find_outs(self, path, *args, **kwargs):
        outs = self.repo.find_outs_by_path(path, *args, **kwargs)

        def _is_cached(out):
            return out.use_cache

        outs = list(filter(_is_cached, outs))
        if not outs:
            raise OutputNotFoundError(path, self.repo)

        return outs

    def open(self, path, mode="r", encoding="utf-8"):
        try:
            outs = self.find_outs(path, strict=False)
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
            self.find_outs(path, strict=False, recursive=True)
            return True
        except OutputNotFoundError:
            return False

    def isdir(self, path):
        if not self.exists(path):
            return False

        path_info = PathInfo(os.path.abspath(path))
        outs = self.find_outs(path, strict=False, recursive=True)
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
        outs = self.find_outs(top, recursive=True, strict=False)

        trie = Trie()

        for out in outs:
            trie[out.path_info.parts] = out

        yield from self._walk(root, trie, topdown=topdown)

    def isdvc(self, path):
        try:
            return len(self.find_outs(path, recursive=True, strict=False)) == 1
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

    def open(self, path, mode="r", encoding="utf-8"):
        if self.dvctree and self.dvctree.exists(path):
            try:
                return self.dvctree.open(path, mode=mode, encoding=encoding)
            except FileNotFoundError:
                pass
        return self.repo.tree.open(path, mode=mode, encoding=encoding)

    def exists(self, path):
        return self.repo.tree.exists(path) or (
            self.dvctree and self.dvctree.exists(path)
        )

    def isdir(self, path):
        return self.repo.tree.isdir(path) or (
            self.dvctree and self.dvctree.isdir(path)
        )

    def isdvc(self, path):
        return self.dvctree is not None and self.dvctree.isdvc(path)

    def isfile(self, path):
        return self.repo.tree.isfile(path) or (
            self.dvctree and self.dvctree.isfile(path)
        )

    def isexec(self, path):
        return self.repo.tree.isexec(path)

    def _walk_one(self, walk):
        try:
            root, dirs, files = next(walk)
        except StopIteration:
            return
        yield root, dirs, files
        for _ in dirs:
            yield from self._walk_one(walk)

    def _walk(self, dvc_walk, repo_walk):
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
        files = set(dvc_fnames)
        for filename in repo_fnames:
            files.add(filename)

        yield repo_root, dirs, list(files)

        # set dir order for next recursion level - shared dirs first so that
        # next() for both generators recurses into the same shared directory
        dvc_dirs[:] = [dirname for dirname in dirs if dirname in dvc_set]
        repo_dirs[:] = [dirname for dirname in dirs if dirname in repo_set]

        for dirname in dirs:
            if dirname in shared:
                yield from self._walk(dvc_walk, repo_walk)
            elif dirname in dvc_set:
                yield from self._walk_one(dvc_walk)
            elif dirname in repo_set:
                yield from self._walk_one(repo_walk)

    def walk(self, top, topdown=True):
        """Walk and merge both DVC and repo trees."""
        assert topdown

        if not self.exists(top):
            raise FileNotFoundError

        if not self.isdir(top):
            raise NotADirectoryError

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

        dvc_walk = self.dvctree.walk(top, topdown=topdown)
        repo_walk = self.repo.tree.walk(top, topdown=topdown)
        yield from self._walk(dvc_walk, repo_walk)

    def _get_used_cache(self, path_info):
        try:
            (out,) = self.dvctree.find_outs(path_info, strict=False)
            filter_info = path_info.relative_to(out.path_info)
            if out.changed_cache(filter_info=filter_info):
                return out.get_used_cache()
        except OutputNotFoundError:
            pass
        return None

    def fetch(self, path, cache, save_git=False, recursive=False, **kwargs):
        """Fetch contents of path into the specified cache.

        If save_git is True, git-only files will be saved to the cache.
        """
        if not self.exists(path):
            raise FileNotFoundError

        path = PathInfo(path)
        downloaded, failed = 0, 0
        used = None

        if self.isdvc(path):
            used = self._get_used_cache(path)
        elif self.isfile(path):
            # git file
            if save_git:
                d, f = self._save_git(path, cache)
                downloaded += d
                failed += f
        else:
            # git dir
            d, f, recursive_used = self._fetch_dir(
                path, cache, save_git, recursive
            )
            downloaded += d
            failed += f
            if recursive_used:
                used.update(recursive_used)

        if used:
            try:
                # pull using the specified cache (not necessarily the default
                # erepo tmpdir cache)
                remote = self.repo.cloud.get_remote(None, "pull")
                downloaded += cache.pull(used, remote=remote, **kwargs)
            except DownloadError as exc:
                failed += exc.amount

        return downloaded, failed

    def _fetch_dir(self, path, cache, save_git, recursive):
        downloaded, failed = 0, 0

        for root, dirs, files in self.walk(path):
            root_path = PathInfo(root)
            for name in dirs + files:
                if name == Repo.DVC_DIR:
                    # import from subrepos currently unsupported
                    raise RecursiveImportError(
                        path.relative_to(self.repo.root_dir), subrepo=True
                    )
                if self.isdvc(root_path / name) and not recursive:
                    raise RecursiveImportError(
                        path.relative_to(self.repo.root_dir)
                    )

        if save_git:
            d, f = self._save_git(path, cache)
            downloaded += d
            failed += f

        return downloaded, failed, None

    def _save_git(self, path, cache):
        downloaded, failed = 0, 0

        info = {cache.PARAM_CHECKSUM: self.get_checksum(path, cache)}
        if info.get(cache.PARAM_CHECKSUM) is None:
            logger.exception(
                "failed to fetch '{}' from '{}' repo".format(
                    path, self.repo.url
                )
            )
            failed += 1
        elif cache.changed_cache(info[cache.PARAM_CHECKSUM]):
            cache.save(path, info, save_link=False, tree=self)
            logger.debug(
                "fetched '{}' from '{}' repo".format(path, self.repo.url)
            )
            if self.isdir(path):
                downloaded += len(
                    [name for name in self.repo.tree.walk_files(path)]
                )
            else:
                downloaded += 1

        return downloaded, failed

    def get_checksum(self, path, cache):
        if self.isfile(path):
            return self._file_checksum(path)
        return self._dir_checksum(path, cache)

    def _dir_checksum(self, path, cache):
        return cache.get_dir_checksum(
            path, tree=self, checksum_func=self._file_checksum
        )

    def _file_checksum(self, path):
        return file_md5(path, self.repo.tree)[0]

    def copyfile(self, src, dest):
        """Copy specified file from this tree to the destination path."""
        if not self.exists(src):
            raise FileNotFoundError

        with self.open(src, mode="rb", encoding=None) as fobj:
            copy_obj_to_file(fobj, dest)

    def checkout(self, path, dest, cache):
        """Checkout the specified path to dest.

        If path is a DVC out, it will be checkout-ed from the specified cache
        to dest. Git only files will be copied directly from tree.
        """
        if not self.exists(path):
            raise FileNotFoundError

        path = PathInfo(path)
        dest = PathInfo(dest)

        if self.isdvc(path):
            self._checkout_dvc(path, dest, cache)
            return

        if self.isfile(path):
            self.copyfile(path, dest)
            return

        for root, _, files in self.walk(path):
            root_path = PathInfo(root)
            dest_dir = dest / root_path.relative_to(path)
            if not os.path.exists(dest_dir):
                makedirs(dest_dir)
            for filename in files:
                src_file = root_path / filename
                dest_file = dest_dir / filename
                if is_valid_filename(filename):
                    name, _ = os.path.splitext(filename)
                    if self.dvctree and not self.dvctree.exists(
                        root_path / name
                    ):
                        self.copyfile(src_file, dest_file)
                else:
                    self.copyfile(src_file, dest_file)

    def _checkout_dvc(self, path, dest, cache):
        """Checkout specified DVC out to dest."""
        try:
            (out,) = self.dvctree.find_outs(path, strict=False)
        except OutputNotFoundError:
            raise FileNotFoundError

        # checkout out to tmp dir, move contents to dest, cleanup tmp dir
        tmp = PathInfo(tmp_fname(dest))
        src = tmp / path.relative_to(out.path_info)
        out.path_info = tmp

        if cache.changed_cache(
            out.info[cache.PARAM_CHECKSUM], filter_info=src
        ):
            raise FileNotFoundError

        try:
            cache.checkout(
                tmp, out.info, filter_info=src,
            )
            move(src, dest)
        except CheckoutError:
            raise FileNotFoundError
        finally:
            remove(tmp)
