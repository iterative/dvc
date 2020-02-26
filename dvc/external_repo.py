import logging
import os
import tempfile
from contextlib import contextmanager
from distutils.dir_util import copy_tree
import threading

from funcy import retry, suppress, wrap_with, cached_property

from dvc.path_info import PathInfo
from dvc.compat import fspath
from dvc.repo import Repo
from dvc.config import NoRemoteError, NotDvcRepoError
from dvc.exceptions import NoRemoteInExternalRepoError
from dvc.exceptions import OutputNotFoundError, NoOutputInExternalRepoError
from dvc.exceptions import FileMissingError, PathMissingError
from dvc.utils.fs import remove, fs_copy, move
from dvc.utils import tmp_fname
from dvc.scm.git import Git


logger = logging.getLogger(__name__)


@contextmanager
def external_repo(url, rev=None, for_write=False):
    logger.debug("Creating external repo {}@{}", url, rev)
    path = _cached_clone(url, rev, for_write=for_write)
    try:
        repo = ExternalRepo(path, url)
    except NotDvcRepoError:
        repo = ExternalGitRepo(path, url)

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


class ExternalRepo(Repo):
    def __init__(self, root_dir, url):
        super().__init__(root_dir)
        self.url = url
        self._set_cache_dir()
        self._fix_upstream()

    def pull_to(self, path, to_info):
        """
        Pull the corresponding file or directory specified by `path` and
        checkout it into `to_info`.

        It works with files tracked by Git and DVC, and also local files
        outside the repository.
        """
        out = None
        path_info = PathInfo(self.root_dir) / path

        with suppress(OutputNotFoundError):
            (out,) = self.find_outs_by_path(fspath(path_info), strict=False)

        try:
            if out and out.use_cache:
                self._pull_cached(out, path_info, to_info)
                return

            # Check if it is handled by Git (it can't have an absolute path)
            if os.path.isabs(path):
                raise FileNotFoundError

            fs_copy(fspath(path_info), fspath(to_info))
        except FileNotFoundError:
            raise PathMissingError(path, self.url)

    def _pull_cached(self, out, path_info, dest):
        with self.state:
            tmp = PathInfo(tmp_fname(dest))
            src = tmp / path_info.relative_to(out.path_info)

            out.path_info = tmp

            # Only pull unless all needed cache is present
            if out.changed_cache(filter_info=src):
                self.cloud.pull(out.get_used_cache(filter_info=src))

            failed = out.checkout(filter_info=src)

            move(src, dest)
            remove(tmp)

            if failed:
                raise FileNotFoundError

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

        remote_name = self.config["core"].get("remote")
        src_repo = Repo(self.url)
        try:
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


class ExternalGitRepo:
    def __init__(self, root_dir, url):
        self.root_dir = root_dir
        self.url = url

    @cached_property
    def scm(self):
        return Git(self.root_dir)

    def close(self):
        if "scm" in self.__dict__:
            self.scm.close()

    def find_out_by_relpath(self, path):
        raise OutputNotFoundError(path, self)

    def pull_to(self, path, to_info):
        try:
            # Git handled files can't have absolute path
            if os.path.isabs(path):
                raise FileNotFoundError

            fs_copy(os.path.join(self.root_dir, path), fspath(to_info))
        except FileNotFoundError:
            raise PathMissingError(path, self.url)

    @contextmanager
    def open_by_relpath(self, path, mode="r", encoding=None, **kwargs):
        try:
            abs_path = os.path.join(self.root_dir, path)
            with open(abs_path, mode, encoding=encoding) as fd:
                yield fd
        except FileNotFoundError:
            raise PathMissingError(path, self.url)


def _cached_clone(url, rev, for_write=False):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out. If for_write is set prevents reusing this dir via
    cache.
    """
    if not for_write and Git.is_sha(rev) and (url, rev) in CLONES:
        return CLONES[url, rev]

    clone_path = _clone_default_branch(url, rev)
    rev_sha = Git(clone_path).resolve_rev(rev or "HEAD")

    if not for_write and (url, rev_sha) in CLONES:
        return CLONES[url, rev_sha]

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    logger.debug("erepo: making a copy of {} clone", url)
    copy_tree(clone_path, repo_path)

    # Check out the specified revision
    if rev is not None:
        _git_checkout(repo_path, rev)

    if not for_write:
        CLONES[url, rev_sha] = repo_path
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
                logger.debug("erepo: git pull {}", url)
                git.pull()
        else:
            logger.debug("erepo: git clone {} to a temporary dir", url)
            clone_path = tempfile.mkdtemp("dvc-clone")
            git = Git.clone(url, clone_path)
            CLONES[url] = clone_path
    finally:
        if git:
            git.close()

    return clone_path


def _git_checkout(repo_path, rev):
    logger.debug("erepo: git checkout {}@{}", repo_path, rev)
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
