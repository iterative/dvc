import logging
import os
import tempfile
import threading
from contextlib import contextmanager, suppress
from distutils.dir_util import copy_tree

from funcy import cached_property, retry, wrap_with

from dvc.config import NoRemoteError, NotDvcRepoError
from dvc.exceptions import (
    DownloadError,
    FileMissingError,
    NoOutputInExternalRepoError,
    NoRemoteInExternalRepoError,
    OutputNotFoundError,
    PathMissingError,
    RecursiveImportError,
)
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.repo.tree import RepoTree
from dvc.scm.git import Git
from dvc.scm.tree import is_working_tree
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


@contextmanager
def external_repo(url, rev=None, for_write=False):
    logger.debug("Creating external repo %s@%s", url, rev)
    path = _cached_clone(url, rev, for_write=for_write)
    if not rev:
        rev = "HEAD"
    try:
        repo = ExternalRepo(path, url, rev, for_write=for_write)
    except NotDvcRepoError:
        repo = ExternalGitRepo(path, url, rev)

    try:
        yield repo
    except NoRemoteError:
        raise NoRemoteInExternalRepoError(url)
    except OutputNotFoundError as exc:
        if exc.repo is repo:
            raise NoOutputInExternalRepoError(exc.output, repo.root_dir, url)
        raise
    except FileMissingError as exc:
        raise PathMissingError(exc.path, url)
    finally:
        repo.close()
        if for_write:
            _remove(path)


CLONES = {}
CACHE_DIRS = {}


def clean_repos():
    # Outside code should not see cache while we are removing
    paths = list(CLONES.values()) + list(CACHE_DIRS.values())
    CLONES.clear()
    CACHE_DIRS.clear()

    for path in paths:
        _remove(path)


class BaseExternalRepo:
    _tree_rev = None
    _local_cache = None

    @contextmanager
    def use_cache(self, cache):
        """Use specified cache instead of erepo tmpdir cache."""
        self._local_cache = cache
        if hasattr(self, "cache"):
            save_cache = self.cache.local
            self.cache.local = cache
        # make cache aware of our repo tree
        with cache.erepo_tree(self.tree):
            yield
        if hasattr(self, "cache"):
            self.cache.local = save_cache
        self._local_cache = None

    @cached_property
    def repo_tree(self):
        return RepoTree(self)

    def get_rev(self):
        """Return current SCM revision"""
        if self._tree_rev:
            return self._tree_rev
        return self.scm.get_rev()

    @contextmanager
    def _open_by_repo_tree_relpath(
        self, path, mode="r", encoding=None, **kwargs
    ):
        try:
            if (
                encoding is None
                and "b" not in mode
                and not is_working_tree(self.tree)
            ):
                # GitTree requires text encoding be set for non-binary mode
                encoding = "utf-8"
            with self.repo_tree.open(
                PathInfo(self.root_dir) / path, mode, encoding=encoding
            ) as fd:
                yield fd
        except FileNotFoundError:
            raise PathMissingError(path, self.url)

    def get_checksum(self, path):
        raise NotImplementedError

    def check_recursive_imports(self, path):
        """Raise RecursiveImportError if path_info contains recursively added
        DVC outs.
        """
        path_info = PathInfo(self.root_dir) / path
        self._recursive_outputs(path_info)

    def _recursive_outputs(self, path_info, recursive=False):
        # if path_info is a non-dvc directory, we need to check for
        # recursively added dvc files
        fetch_infos = []
        for root, dirs, files in self.repo_tree.walk(path_info):
            root_path = PathInfo(root)
            for name in dirs + files:
                if name == Repo.DVC_DIR:
                    # import from subrepos currently unsupported
                    raise RecursiveImportError(
                        path_info.relative_to(self.root_dir), subrepo=True
                    )
                if self.repo_tree.isdvc(root_path / name):
                    if recursive:
                        fetch_infos.append(self._fetch_info(root_path / name))
                    else:
                        raise RecursiveImportError(
                            path_info.relative_to(self.root_dir)
                        )
        return fetch_infos

    def _fetch_info(self, path_info):
        if self.repo_tree.isdvc(path_info):
            (out,) = self.find_outs_by_path(path_info, strict=False)
            filter_info = path_info.relative_to(out.path_info)
        else:
            filter_info = None
        return path_info, filter_info

    def get_external(self, path, to_info, recursive=False, **kwargs):
        """
        Pull the corresponding file or directory specified by `path` and
        into `to_info`.

        It works with files tracked by Git and DVC, and also local files
        outside the repository.
        """
        path_info = PathInfo(self.root_dir) / path

        if not self.repo_tree.exists(path_info):
            raise PathMissingError(path, self.url)

        fetch_infos = [self._fetch_info(path_info)]
        if self.repo_tree.isdir(path_info) and not self.repo_tree.isdvc(
            path_info
        ):
            fetch_infos.extend(self._recursive_outputs(path_info, recursive))

        self._fetch_external(fetch_infos, **kwargs)

        with self.state:
            self.repo_tree.copytree(path_info, to_info)

    def fetch_external(self, files, **kwargs):
        """Fetch erepo files into the specified cache.

        Works with files tracked by Git and DVC.
        """
        files = [(PathInfo(self.root_dir) / name, None) for name in files]
        return self._fetch_external(files, **kwargs)

    def _fetch_external(self, fetch_infos, **kwargs):
        downloaded, failed = 0, 0

        with self.state:
            for path_info, filter_info in fetch_infos:
                if self.repo_tree.isdvc(path_info):
                    (out,) = self.find_outs_by_path(path_info, strict=False)
                    d, f = self._fetch_out(
                        out, filter_info=filter_info, **kwargs
                    )
                else:
                    d, f = self._fetch_git(path_info)
                downloaded += d
                failed += f

        if failed:
            logger.exception(
                "failed to fetch '{}' files from '{}' repo".format(
                    failed, self.url
                )
            )
        return downloaded, failed

    def _fetch_out(self, out, filter_info=None, **kwargs):
        """Fetch specified erepo out."""
        downloaded, failed = 0, 0
        if out.changed_cache(filter_info=filter_info):
            used_cache = out.get_used_cache()
            try:
                downloaded += self.cloud.pull(used_cache, **kwargs)
            except DownloadError as exc:
                failed += exc.amount
        return downloaded, failed

    def _fetch_git(self, path_info):
        """Copy git tracked file into specified cache."""
        downloaded, failed = 0, 0
        if hasattr(self, "cache"):
            local_cache = self.cache.local
        elif self._local_cache:
            local_cache = self._local_cache
        else:
            return downloaded, failed

        info = local_cache.save_info(path_info)
        if info.get(local_cache.PARAM_CHECKSUM) is None:
            logger.exception(
                "failed to fetch '{}' from '{}' repo".format(
                    path_info, self.url
                )
            )
            failed += 1
        elif local_cache.changed_cache(info[local_cache.PARAM_CHECKSUM]):
            local_cache.save_tree(self.repo_tree, path_info, info)
            logger.debug(
                "fetched '{}' from '{}' repo".format(path_info, self.url)
            )
            downloaded += 1
        return downloaded, failed


class ExternalRepo(Repo, BaseExternalRepo):
    def __init__(self, root_dir, url, rev, for_write=False):
        if for_write:
            super().__init__(root_dir)
        else:
            # use GitTree instead of WorkingTree
            root_dir = os.path.realpath(root_dir)
            scm = Git(root_dir)
            tree = scm.get_tree(rev)
            self._tree_rev = tree.rev

            if not tree.isdir(os.path.join(root_dir, self.DVC_DIR)):
                raise NotDvcRepoError("'{}' is not a DVC repo".format(url))

            super().__init__(root_dir, find_root=False, scm=scm, tree=tree)

        self.url = url
        self._set_cache_dir()
        self._fix_upstream()

    @wrap_with(threading.Lock())
    def _set_cache_dir(self):
        try:
            cache_dir = CACHE_DIRS[self.url]
        except KeyError:
            cache_dir = CACHE_DIRS[self.url] = tempfile.mkdtemp("dvc-cache")
        self.cache.local.cache_dir = cache_dir

    def _fix_upstream(self):
        if not os.path.isdir(self.url):
            return

        try:
            src_repo = Repo(self.url)
        except NotDvcRepoError:
            # If ExternalRepo does not throw NotDvcRepoError and Repo does,
            # the self.url might be a bare git repo.
            # NOTE: This will fail to resolve remote with relative path,
            # same as if it was a remote DVC repo.
            return

        try:
            remote_name = self.config["core"].get("remote")
            if remote_name:
                self._fix_local_remote(src_repo, remote_name)
            else:
                self._add_upstream(src_repo)
        finally:
            src_repo.close()

    def _fix_local_remote(self, src_repo, remote_name):
        # If a remote URL is relative to the source repo,
        # it will have changed upon config load and made
        # relative to this new repo. Restore the old one here.
        new_remote = self.config["remote"][remote_name]
        old_remote = src_repo.config["remote"][remote_name]
        if new_remote["url"] != old_remote["url"]:
            new_remote["url"] = old_remote["url"]

    def _add_upstream(self, src_repo):
        # Fill the empty upstream entry with a new remote pointing to the
        # original repo's cache location.
        cache_dir = src_repo.cache.local.cache_dir
        self.config["remote"]["auto-generated-upstream"] = {"url": cache_dir}
        self.config["core"]["remote"] = "auto-generated-upstream"

    def get_checksum(self, path):
        return self.cache.local.get_checksum(path)

    @contextmanager
    def open_by_relpath(self, path, remote=None, mode="r", encoding=None):
        """Opens a specified resource as a file object."""
        path_info = PathInfo(self.root_dir) / path
        if self.repo_tree.isdvc(path_info):
            (out,) = self.find_outs_by_path(path_info, strict=False)
            if out.use_cache:
                try:
                    with self._open_cached(out, remote, mode, encoding) as fd:
                        yield fd
                    return
                except FileNotFoundError as exc:
                    raise FileMissingError(path) from exc

        with self._open_by_repo_tree_relpath(
            path, mode=mode, encoding=encoding
        ) as fobj:
            yield fobj


class ExternalGitRepo(BaseExternalRepo):
    state = suppress()

    def __init__(self, root_dir, url, rev):
        self.root_dir = os.path.realpath(root_dir)
        self.url = url
        self.tree = self.scm.get_tree(rev)
        self._tree_rev = self.tree.rev

    @cached_property
    def scm(self):
        return Git(self.root_dir)

    def close(self):
        if "scm" in self.__dict__:
            self.scm.close()

    def find_out_by_relpath(self, path):
        raise OutputNotFoundError(path, self)

    @contextmanager
    def open_by_relpath(self, path, mode="r", encoding=None, **kwargs):
        with self._open_by_repo_tree_relpath(
            path, mode=mode, encoding=encoding, **kwargs
        ) as fobj:
            yield fobj

    def get_checksum(self, path):
        return self._local_cache.get_checksum(path)


def _cached_clone(url, rev, for_write=False):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out. If for_write is set prevents reusing this dir via
    cache.
    """
    clone_path = _clone_default_branch(url, rev)

    if not for_write and (url) in CLONES:
        return CLONES[url]

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    logger.debug("erepo: making a copy of %s clone", url)
    copy_tree(clone_path, repo_path)

    if for_write:
        _git_checkout(repo_path, rev)
    else:
        CLONES[url] = repo_path
    return repo_path


@wrap_with(threading.Lock())
def _clone_default_branch(url, rev):
    """Get or create a clean clone of the url.

    The cloned is reactualized with git pull unless rev is a known sha.
    """
    clone_path = CLONES.get(url)

    git = None
    try:
        if clone_path:
            git = Git(clone_path)
            # Do not pull for known shas, branches and tags might move
            if not Git.is_sha(rev) or not git.has_rev(rev):
                logger.debug("erepo: git pull %s", url)
                git.pull()
        else:
            logger.debug("erepo: git clone %s to a temporary dir", url)
            clone_path = tempfile.mkdtemp("dvc-clone")
            git = Git.clone(url, clone_path)
            CLONES[url] = clone_path
    finally:
        if git:
            git.close()

    return clone_path


def _git_checkout(repo_path, rev):
    logger.debug("erepo: git checkout %s@%s", repo_path, rev)
    git = Git(repo_path)
    try:
        git.checkout(rev)
    finally:
        git.close()


def _remove(path):
    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        os_retry(remove)(path)
    else:
        remove(path)
