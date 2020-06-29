import logging
import os
import tempfile
import threading
from contextlib import contextmanager
from distutils.dir_util import copy_tree
from typing import Iterable

from funcy import cached_property, retry, wrap_with

from dvc.config import NoRemoteError, NotDvcRepoError
from dvc.exceptions import (
    FileMissingError,
    NoOutputInExternalRepoError,
    NoRemoteInExternalRepoError,
    OutputNotFoundError,
    PathMissingError,
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
    # pylint: disable=no-member

    _local_cache = None

    @property
    def local_cache(self):
        if hasattr(self, "cache"):
            return self.cache.local
        return self._local_cache

    @contextmanager
    def use_cache(self, cache):
        """Use the specified cache in place of default tmpdir cache for
        download operations.
        """
        if hasattr(self, "cache"):
            save_cache = self.cache.local
            self.cache.local = cache
        self._local_cache = cache

        yield

        if hasattr(self, "cache"):
            self.cache.local = save_cache
        self._local_cache = None

    @cached_property
    def repo_tree(self):
        return RepoTree(self, fetch=True)

    def get_rev(self):
        if is_working_tree(self.tree):
            return self.scm.get_rev()
        if hasattr(self.tree, "tree"):
            return self.tree.tree.rev
        return self.tree.rev

    def fetch_external(self, paths: Iterable, **kwargs):
        """Fetch specified external repo paths into cache.

        Returns 3-tuple in the form
            (downloaded, failed, list(cache_infos))
        where cache_infos can be used as checkout targets for the
        fetched paths.
        """
        download_results = []
        failed = 0

        paths = [PathInfo(self.root_dir) / path for path in paths]

        def download_update(result):
            download_results.append(result)

        save_infos = []
        for path in paths:
            if not self.repo_tree.exists(path):
                raise PathMissingError(path, self.url)
            save_info = self.local_cache.save(
                path,
                self.repo_tree,
                None,
                save_link=False,
                download_callback=download_update,
            )
            save_infos.append(save_info)

        return sum(download_results), failed, save_infos

    def get_external(self, path, dest):
        """Convenience wrapper for fetch_external and checkout."""
        if self.local_cache:
            # fetch DVC and git files to tmpdir cache, then checkout
            _, _, save_infos = self.fetch_external([path])
            self.local_cache.checkout(PathInfo(dest), save_infos[0])
        else:
            # git-only erepo with no cache, just copy files directly
            # to dest
            path = PathInfo(self.root_dir) / path
            if not self.repo_tree.exists(path):
                raise PathMissingError(path, self.url)
            self.repo_tree.copytree(path, dest)


class ExternalRepo(Repo, BaseExternalRepo):
    def __init__(self, root_dir, url, rev, for_write=False):
        if for_write:
            super().__init__(root_dir)
        else:
            root_dir = os.path.realpath(root_dir)
            super().__init__(root_dir, scm=Git(root_dir), rev=rev)
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
        self._local_cache = self.cache.local

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


class ExternalGitRepo(BaseExternalRepo):
    def __init__(self, root_dir, url, rev):
        self.root_dir = os.path.realpath(root_dir)
        self.url = url
        self.tree = self.scm.get_tree(rev)

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
        """Opens a specified resource as a file object."""
        tree = RepoTree(self)
        try:
            with tree.open(
                path, mode=mode, encoding=encoding, **kwargs
            ) as fobj:
                yield fobj
        except FileNotFoundError:
            raise PathMissingError(path, self.url)


def _cached_clone(url, rev, for_write=False):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out. If for_write is set prevents reusing this dir via
    cache.
    """
    # even if we have already cloned this repo, we may need to
    # fetch/fast-forward to get specified rev
    clone_path = _clone_default_branch(url, rev)

    if not for_write and (url) in CLONES:
        return CLONES[url]

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    logger.debug("erepo: making a copy of %s clone", url)
    copy_tree(clone_path, repo_path)

    # Check out the specified revision
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
