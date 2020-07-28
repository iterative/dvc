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
from dvc.scm.base import CloneError
from dvc.scm.git import Git
from dvc.tree import LocalTree
from dvc.utils.fs import remove

logger = logging.getLogger(__name__)


def _is_dvc_main_repo(scm, path, rev):
    isdir = scm.get_tree(rev).isdir if scm else os.path.isdir
    return isdir(os.path.join(path, Repo.DVC_DIR))


@contextmanager
def external_repo(url, rev=None, for_write=False, **kwargs):
    logger.debug("Creating external repo %s@%s", url, rev)
    path = _cached_clone(url, rev, for_write=for_write)
    path = os.path.realpath(path)

    if not rev:
        # Local HEAD points to the tip of whatever branch we first cloned from
        # (which may not be the default branch), use origin/HEAD here to get
        # the tip of the default branch
        rev = "refs/remotes/origin/HEAD"
    if for_write:
        rev = None
        scm = None
    else:
        scm = Git(path)

    erepo_cls = (
        ExternalDVCRepo
        if _is_dvc_main_repo(scm, path, rev)
        else ExternalGitRepo
    )
    repo_kw = dict(scm=scm, rev=rev, for_write=for_write, url=url, **kwargs)
    repo = erepo_cls(path, **repo_kw)

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
    paths = [path for path, _ in CLONES.values()] + list(CACHE_DIRS.values())
    CLONES.clear()
    CACHE_DIRS.clear()

    for path in paths:
        _remove(path)


def _fix_local_remote(orig_repo, src_repo, remote_name):
    # If a remote URL is relative to the source repo,
    # it will have changed upon config load and made
    # relative to this new repo. Restore the old one here.
    new_remote = orig_repo.config["remote"][remote_name]
    old_remote = src_repo.config["remote"][remote_name]
    if new_remote["url"] != old_remote["url"]:
        new_remote["url"] = old_remote["url"]


def _add_upstream(orig_repo, src_repo):
    # Fill the empty upstream entry with a new remote pointing to the
    # original repo's cache location.
    orig_repo.config["remote"]["auto-generated-upstream"] = {
        "url": src_repo.cache.local.cache_dir
    }
    orig_repo.config["core"]["remote"] = "auto-generated-upstream"


class BaseExternalMixin:
    @wrap_with(threading.Lock())
    def _setup_cache_dir(self):
        # share same cache_dir among all subrepos
        try:
            self.cache_dir = CACHE_DIRS[self.url]
        except KeyError:
            self.cache_dir = CACHE_DIRS[self.url] = tempfile.mkdtemp(
                "dvc-cache"
            )

    def _fix_upstream(self, orig_repo):
        try:
            rel_path = os.path.relpath(orig_repo.root_dir, self.root_dir)
            src_repo = Repo(PathInfo(self.url) / rel_path)
        except NotDvcRepoError:
            # If ExternalRepo does not throw NotDvcRepoError and Repo does,
            # the self.url might be a bare git repo.
            # NOTE: This will fail to resolve remote with relative path,
            # same as if it was a remote DVC repo.
            return

        try:
            remote_name = orig_repo.config["core"].get("remote")
            if remote_name:
                _fix_local_remote(orig_repo, src_repo, remote_name)
            else:
                _add_upstream(orig_repo, src_repo)
        finally:
            src_repo.close()

    def close(self):
        if self.scm:
            self.scm.close()

    def fetch_external(self, paths: Iterable, cache, **kwargs):
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
            save_info = cache.save(
                path,
                self.repo_tree,
                None,
                save_link=False,
                download_callback=download_update,
            )
            save_infos.append(save_info)

        return sum(download_results), failed, save_infos

    def get_external(self, src, dest):
        """Convenience wrapper for fetch_external and checkout."""
        repo = self.in_repo(src)
        if repo:
            cache = repo.cache.local
            _, _, save_infos = self.fetch_external([src], cache)
            cache.checkout(PathInfo(dest), save_infos[0])
        else:
            path = PathInfo(self.root_dir) / src
            if not self.repo_tree.exists(path):
                raise PathMissingError(src, self.url)
            self.repo_tree.copytree(path, dest)

    def get_rev(self):
        if isinstance(self.tree, LocalTree):
            return self.scm.get_rev()
        return self.tree.rev

    def get_checksum(self, path_info, cache):
        if self.repo_tree.isdir(path_info):
            return cache.tree.get_hash(path_info, tree=self.repo_tree)
        return self.repo_tree.get_file_hash(path_info)

    def in_repo(self, path):
        tree = self.repo_tree.in_subtree(PathInfo(self.root_dir) / path)
        return tree.repo if tree else None

    @cached_property
    def main_tree(self):
        # FIXME: Repo.tree has dvcignore embedded on it, which might ignore
        #  subrepos. Also, there might be unwanted side effects of using `tree`
        #  that does not ignore subrepos, so we create our own `master` tree,
        #  that speaks for the whole repository
        #   ---
        #  This is blocking implementation of subrepos inside `Repo`,
        #  as collecting `.dvcignore` twice is not an answer.
        #  we need very granular controls on dvcignore, such that we have
        #  per-ops controls and also be able to create a separate instance
        #  of tree that does have some attributes of dvcignore (en/dis)abled.
        kwargs = dict(
            use_dvcignore=True,
            dvcignore_root=self.root_dir,
            ignore_subrepo=False,
        )
        if self.for_write:
            return LocalTree(None, {"url": self.root_dir}, **kwargs)
        return self.scm.get_tree(rev=self.rev, **kwargs)

    @cached_property
    def repo_tree(self) -> "RepoTree":
        return RepoTree(
            self,
            self.main_tree,
            traverse_subrepo=True,
            repo_constructor=self.make_subrepo,
            **self._tree_config,
        )

    def make_subrepo(self, path):
        repo = Repo(path, scm=self.scm, rev=self.rev)
        repo.cache.local.cache_dir = self.cache_dir
        if self.cache_types:
            repo.cache.local.cache_types = self.cache_types
        if os.path.isdir(self.url):
            self._fix_upstream(repo)
        return repo


class ExternalDVCRepo(BaseExternalMixin, Repo):
    def __init__(
        self,
        root_dir,
        scm=None,
        rev=None,
        for_write=False,
        url=None,
        fetch=True,
        cache_dir=None,
        cache_types=None,
        **kwargs,
    ):
        super().__init__(root_dir, scm=scm, rev=rev)

        self.url = url
        self.rev = rev
        self.for_write = for_write
        self._tree_config = {"fetch": fetch, **kwargs}
        self.cache_types = cache_types
        if cache_types:
            self.cache.local.cache_types = cache_types
        if cache_dir:
            self.cache_dir = cache_dir
        else:
            self._setup_cache_dir()
        self.cache.local.cache_dir = self.cache_dir
        if os.path.isdir(self.url):
            self._fix_upstream(self)


class ExternalGitRepo(BaseExternalMixin):
    def __init__(
        self,
        root_dir,
        scm=None,
        rev=None,
        for_write=False,
        url=None,
        fetch=True,
        cache_dir=None,
        cache_types=None,
        **kwargs,
    ):
        self.root_dir = root_dir
        self.scm = scm
        self.url = url
        self.rev = rev
        self.for_write = for_write
        self._tree_config = {"fetch": fetch, **kwargs}
        self.cache_types = cache_types
        if cache_dir:
            self.cache_dir = cache_dir
        else:
            self._setup_cache_dir()

    @property
    def tree(self):
        return self.main_tree


def _cached_clone(url, rev, for_write=False):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out. If for_write is set prevents reusing this dir via
    cache.
    """
    # even if we have already cloned this repo, we may need to
    # fetch/fast-forward to get specified rev
    clone_path, shallow = _clone_default_branch(url, rev, for_write=for_write)

    if not for_write and (url) in CLONES:
        return CLONES[url][0]

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    logger.debug("erepo: making a copy of %s clone", url)
    copy_tree(clone_path, repo_path)

    # Check out the specified revision
    if for_write:
        _git_checkout(repo_path, rev)
    else:
        CLONES[url] = (repo_path, shallow)
    return repo_path


@wrap_with(threading.Lock())
def _clone_default_branch(url, rev, for_write=False):
    """Get or create a clean clone of the url.

    The cloned is reactualized with git pull unless rev is a known sha.
    """
    clone_path, shallow = CLONES.get(url, (None, False))

    git = None
    try:
        if clone_path:
            git = Git(clone_path)
            # Do not pull for known shas, branches and tags might move
            if not Git.is_sha(rev) or not git.has_rev(rev):
                if shallow:
                    # If we are missing a rev in a shallow clone, fallback to
                    # a full (unshallowed) clone. Since fetching specific rev
                    # SHAs is only available in certain git versions, if we
                    # have need to reference multiple specific revs for a
                    # given repo URL it is easier/safer for us to work with
                    # full clones in this case.
                    logger.debug("erepo: unshallowing clone for '%s'", url)
                    _unshallow(git)
                    shallow = False
                    CLONES[url] = (clone_path, shallow)
                else:
                    logger.debug("erepo: git pull '%s'", url)
                    git.pull()
        else:
            logger.debug("erepo: git clone '%s' to a temporary dir", url)
            clone_path = tempfile.mkdtemp("dvc-clone")
            if not for_write and rev and not Git.is_sha(rev):
                # If rev is a tag or branch name try shallow clone first
                try:
                    git = Git.clone(url, clone_path, shallow_branch=rev)
                    shallow = True
                    logger.debug(
                        "erepo: using shallow clone for branch '%s'", rev
                    )
                except CloneError:
                    pass
            if not git:
                git = Git.clone(url, clone_path)
                shallow = False
            CLONES[url] = (clone_path, shallow)
    finally:
        if git:
            git.close()

    return clone_path, shallow


def _unshallow(git):
    if git.repo.head.is_detached:
        # If this is a detached head (i.e. we shallow cloned a tag) switch to
        # the default branch
        origin_refs = git.repo.remotes["origin"].refs
        ref = origin_refs["HEAD"].reference
        branch_name = ref.name.split("/")[-1]
        branch = git.repo.create_head(branch_name, ref)
        branch.set_tracking_branch(ref)
        branch.checkout()
    git.pull(unshallow=True)


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
