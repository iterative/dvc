import logging
import os
import tempfile
import threading
from contextlib import contextmanager
from typing import Dict, Iterable

from funcy import cached_property, reraise, retry, wrap_with

from dvc.exceptions import (
    DvcException,
    FileMissingError,
    NoOutputInExternalRepoError,
    NoRemoteInExternalRepoError,
    NotDvcRepoError,
    OutputNotFoundError,
    PathMissingError,
)
from dvc.path_info import PathInfo
from dvc.repo import Repo
from dvc.utils import relpath

logger = logging.getLogger(__name__)


class IsADVCRepoError(DvcException):
    """Raised when it is not expected to be a dvc repo."""


@contextmanager
def external_repo(url, rev=None, for_write=False, **kwargs):
    from dvc.config import NoRemoteError
    from dvc.scm.git import Git

    logger.debug("Creating external repo %s@%s", url, rev)
    path = _cached_clone(url, rev, for_write=for_write)
    # Local HEAD points to the tip of whatever branch we first cloned from
    # (which may not be the default branch), use origin/HEAD here to get
    # the tip of the default branch
    rev = rev or "refs/remotes/origin/HEAD"

    root_dir = path if for_write else os.path.realpath(path)
    conf = dict(
        root_dir=root_dir,
        url=url,
        scm=None if for_write else Git(root_dir),
        rev=None if for_write else rev,
        for_write=for_write,
        uninitialized=True,
        **kwargs,
    )
    repo = ExternalRepo(**conf)

    try:
        yield repo
    except NoRemoteError as exc:
        raise NoRemoteInExternalRepoError(url) from exc
    except OutputNotFoundError as exc:
        if exc.repo is repo:
            raise NoOutputInExternalRepoError(
                exc.output, repo.root_dir, url
            ) from exc
        raise
    except FileMissingError as exc:
        raise PathMissingError(exc.path, url) from exc
    finally:
        repo.close()
        if for_write:
            _remove(path)


CLONES: Dict[str, str] = {}
CACHE_DIRS: Dict[str, str] = {}


def clean_repos():
    # Outside code should not see cache while we are removing
    paths = [path for path, _ in CLONES.values()] + list(CACHE_DIRS.values())
    CLONES.clear()
    CACHE_DIRS.clear()

    for path in paths:
        _remove(path)


def _get_remote_config(url):
    try:
        repo = Repo(url)
    except NotDvcRepoError:
        return {}

    try:
        name = repo.config["core"].get("remote")
        if not name:
            # Fill the empty upstream entry with a new remote pointing to the
            # original repo's cache location.
            name = "auto-generated-upstream"
            return {
                "core": {"remote": name},
                "remote": {name: {"url": repo.cache.local.cache_dir}},
            }

        # Use original remote to make sure that we are using correct url,
        # credential paths, etc if they are relative to the config location.
        return {"remote": {name: repo.config["remote"][name]}}
    finally:
        repo.close()


class ExternalRepo(Repo):
    # pylint: disable=no-member

    def __init__(
        self,
        root_dir,
        url,
        scm=None,
        rev=None,
        for_write=False,
        cache_dir=None,
        cache_types=None,
        uninitialized=False,
        **kwargs,
    ):
        self.url = url
        self.for_write = for_write
        self.tree_confs = kwargs

        self._cache_config = {
            "cache": {
                "dir": cache_dir or self._get_cache_dir(),
                "type": cache_types,
            }
        }

        config = self._cache_config.copy()
        if os.path.isdir(url):
            config.update(_get_remote_config(url))

        super().__init__(
            root_dir,
            scm=scm,
            rev=rev,
            uninitialized=uninitialized,
            config=config,
        )

    def __str__(self):
        return self.url

    @cached_property
    def repo_tree(self):
        return self._get_tree_for(
            self, subrepos=not self.for_write, repo_factory=self.make_repo
        )

    def get_rev(self):
        from dvc.tree.local import LocalTree

        assert self.scm
        if isinstance(self.tree, LocalTree):
            return self.scm.get_rev()
        return self.tree.rev

    def _fetch_to_cache(self, path_info, repo, callback, **kwargs):
        hash_info = self.repo_tree.get_hash(
            path_info,
            download_callback=callback,
            follow_subrepos=False,
            **kwargs,
        )
        repo.cache.local.save(
            path_info,
            self.repo_tree,
            hash_info,
            save_link=False,
            download_callback=callback,
        )
        return hash_info

    def fetch_external(self, paths: Iterable, **kwargs):
        """Fetch specified external repo paths into cache.

        Returns 3-tuple in the form
            (downloaded, failed, list(cache_infos))
        where cache_infos can be used as checkout targets for the
        fetched paths.
        """
        download_results = []
        failed = 0
        root = PathInfo(self.root_dir)

        paths = [root / path for path in paths]

        def download_update(result):
            download_results.append(result)

        hash_infos = []
        for path in paths:
            with reraise(FileNotFoundError, PathMissingError(path, self.url)):
                metadata = self.repo_tree.metadata(path)

            self._check_repo(path, metadata.repo)
            repo = metadata.repo
            hash_info = self._fetch_to_cache(
                path, repo, download_update, **kwargs
            )
            hash_infos.append(hash_info)

        return sum(download_results), failed, hash_infos

    def _check_repo(self, path_info, repo):
        if not repo:
            return

        repo_path = PathInfo(repo.root_dir)
        if path_info == repo_path and isinstance(repo, Repo):
            message = "Cannot fetch a complete DVC repository"
            if repo.root_dir != self.root_dir:
                rel = relpath(repo.root_dir, self.root_dir)
                message += f" '{rel}'"
            raise IsADVCRepoError(message)

    def get_external(self, path, dest):
        """Convenience wrapper for fetch_external and checkout."""
        path_info = PathInfo(self.root_dir) / path
        with reraise(FileNotFoundError, PathMissingError(path, self.url)):
            metadata = self.repo_tree.metadata(path_info)

        self._check_repo(path_info, metadata.repo)
        if metadata.output_exists:
            repo = metadata.repo
            cache = repo.cache.local
            # fetch DVC and git files to tmpdir cache, then checkout
            save_info = self._fetch_to_cache(path_info, repo, None)
            cache.checkout(PathInfo(dest), save_info)
        else:
            # git-only folder, just copy files directly to dest
            tree = self._get_tree_for(metadata.repo)  # ignore subrepos
            tree.copytree(path_info, dest)

    def _get_tree_for(self, repo, **kwargs):
        """
        Provides a combined tree of a single repo with dvc + git/local tree.
        """
        from dvc.tree.repo import RepoTree

        kw = {**self.tree_confs, **kwargs}
        if "fetch" not in kw:
            kw["fetch"] = True
        return RepoTree(repo, **kw)

    def make_repo(self, path):
        config = self._cache_config.copy()
        if os.path.isdir(self.url):
            rel = os.path.relpath(path, self.root_dir)
            repo_path = os.path.join(self.url, rel)
            config.update(_get_remote_config(repo_path))
        return Repo(path, scm=self.scm, rev=self.get_rev(), config=config)

    @wrap_with(threading.Lock())
    def _get_cache_dir(self):
        try:
            cache_dir = CACHE_DIRS[self.url]
        except KeyError:
            cache_dir = CACHE_DIRS[self.url] = tempfile.mkdtemp("dvc-cache")
        return cache_dir


def _cached_clone(url, rev, for_write=False):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out. If for_write is set prevents reusing this dir via
    cache.
    """
    from distutils.dir_util import copy_tree

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
    from dvc.scm.git import Git

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
                from dvc.scm.base import CloneError

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
    if git.gitpython.repo.head.is_detached:
        # If this is a detached head (i.e. we shallow cloned a tag) switch to
        # the default branch
        origin_refs = git.gitpython.repo.remotes["origin"].refs
        ref = origin_refs["HEAD"].reference
        branch_name = ref.name.split("/")[-1]
        branch = git.gitpython.repo.create_head(branch_name, ref)
        branch.set_tracking_branch(ref)
        branch.checkout()
    git.pull(unshallow=True)


def _git_checkout(repo_path, rev):
    from dvc.scm.git import Git

    logger.debug("erepo: git checkout %s@%s", repo_path, rev)
    git = Git(repo_path)
    try:
        git.checkout(rev)
    finally:
        git.close()


def _remove(path):
    from dvc.utils.fs import remove

    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        try:
            os_retry(remove)(path)
        except PermissionError:
            logger.warning(
                "Failed to remove '%s'", relpath(path), exc_info=True
            )
    else:
        remove(path)
