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
)
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.repo.tree import RepoTree
from dvc.scm.git import Git
from dvc.utils import tmp_fname
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


@contextmanager
def external_repo(
    url, rev=None, for_write=False, cache_dir=None, cache_types=None
):
    logger.debug("Creating external repo %s@%s", url, rev)
    path = _cached_clone(url, rev, for_write=for_write)
    if not rev:
        rev = "HEAD"
    try:
        repo = ExternalRepo(
            path, url, rev, cache_dir=cache_dir, cache_types=cache_types
        )
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
    @cached_property
    def repo_tree(self):
        return RepoTree(self)

    def get_external(self, path, to_info, jobs=None):
        """
        Pull the corresponding file or directory specified by `path` and
        into `to_info`.

        It works with files tracked by Git and DVC, and also local files
        outside the repository.
        """
        path_info = PathInfo(self.root_dir) / path

        if not self.repo_tree.exists(path_info):
            raise PathMissingError(path, self.url)

        # fetch any needed and uncached DVC outs
        if self.repo_tree.isdvc(path_info):
            try:
                self._fetch_dvc_path(path_info, to_info)
            except FileNotFoundError:
                pass

        with self.state:
            self.repo_tree.copytree(path_info, to_info)

    def fetch_external(self, files, jobs=None):
        """Fetch specified erepo files into cache.

        Works with files tracked by Git and DVC.
        """
        downloaded, failed = 0, 0

        with self.state:
            for name in files:
                path_info = PathInfo(self.root_dir) / name
                if self.repo_tree.isdvc(path_info):
                    out = self.find_out_by_relpath(name)
                    d, f = self._fetch_out(out, name, jobs=jobs)
                else:
                    d, f = self._fetch_git(name, path_info)
                downloaded += d
                failed += f

        if failed:
            logger.exception(
                "failed to fetch '{}' files from '{}' repo".format(
                    failed, self.url
                )
            )
        return downloaded, failed

    def _fetch_dvc_path(self, path_info, dest, jobs=None):
        try:
            (out,) = self.find_outs_by_path(path_info, strict=False)
        except OutputNotFoundError:
            raise FileNotFoundError

        with self.state:
            tmp = PathInfo(tmp_fname(dest))
            src = tmp / path_info.relative_to(out.path_info)

            downloaded, failed = self._fetch_out(
                out, path_info, filter_info=src, jobs=jobs
            )
            if failed:
                logger.exception(
                    "failed to fetch '{}' files from '{}' repo".format(
                        failed, self.url
                    )
                )
            return downloaded, failed

    def _fetch_out(self, out, name, filter_info=None, jobs=None):
        """Fetch specified erepo out."""
        downloaded, failed = 0, 0
        if out.changed_cache(filter_info=filter_info):
            used_cache = out.get_used_cache()
            try:
                downloaded += self.cloud.pull(used_cache, jobs=jobs)
            except DownloadError as exc:
                failed += exc.amount
        return downloaded, failed

    def _fetch_git(self, name, path_info):
        """Copy git tracked file into cache."""
        local_cache = self.cache.local
        downloaded, failed = 0, 0
        info = local_cache.save_info(path_info)
        if info.get(local_cache.PARAM_CHECKSUM) is None:
            logger.exception(
                "failed to fetch '{}' from '{}' repo".format(name, self.url)
            )
            failed += 1
        elif local_cache.changed_cache(info[local_cache.PARAM_CHECKSUM]):
            with self.repo_tree.open(path_info, "rb") as fobj:
                local_cache.save_obj(fobj, info)
            logger.debug("fetched '{}' from '{}' repo", name, self.url)
            downloaded += 1
        return downloaded, failed


class ExternalRepo(Repo, BaseExternalRepo):
    def __init__(self, root_dir, url, rev, cache_dir=None, cache_types=None):
        root_dir = os.path.realpath(root_dir)
        scm = Git(root_dir)
        tree = scm.get_tree(rev)

        if not tree.isdir(os.path.join(root_dir, self.DVC_DIR)):
            raise NotDvcRepoError("'{}' is not a DVC repo".format(url))

        super().__init__(root_dir, find_root=False, scm=scm, tree=tree)

        self.url = url
        self._set_cache_dir(cache_dir=cache_dir, cache_types=None)
        self._fix_upstream()

    @wrap_with(threading.Lock())
    def _set_cache_dir(self, cache_dir=None, cache_types=None):
        if not cache_dir:
            try:
                cache_dir = CACHE_DIRS[self.url]
            except KeyError:
                cache_dir = CACHE_DIRS[self.url] = tempfile.mkdtemp(
                    "dvc-cache"
                )
        self.cache.local.cache_dir = cache_dir

        if cache_types:
            self.cache.local.cache_types = cache_types

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

    @cached_property
    def state(self):
        return suppress()

    def close(self):
        if "scm" in self.__dict__:
            self.scm.close()

    def find_out_by_relpath(self, path):
        raise OutputNotFoundError(path, self)

    @contextmanager
    def open_by_relpath(self, path, mode="r", encoding=None, **kwargs):
        """Opens a specified resource as a file object."""
        try:
            abs_path = os.path.join(self.root_dir, path)
            with self.repo_tree.open(abs_path, mode, encoding=encoding) as fd:
                yield fd
        except FileNotFoundError:
            raise PathMissingError(path, self.url)


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
