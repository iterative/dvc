import logging
import os
import tempfile
from contextlib import contextmanager
from distutils.dir_util import copy_tree
import threading

from funcy import retry, suppress, wrap_with, cached_property

from dvc.compat import fspath
from dvc.repo import Repo
from dvc.config import Config, NoRemoteError, NotDvcRepoError
from dvc.exceptions import NoRemoteInExternalRepoError
from dvc.exceptions import OutputNotFoundError, NoOutputInExternalRepoError
from dvc.exceptions import FileMissingError, PathMissingError
from dvc.remote import RemoteConfig
from dvc.utils.fs import remove, fs_copy
from dvc.scm import SCM


logger = logging.getLogger(__name__)


@contextmanager
def external_repo(url, rev=None):
    path = _cached_clone(url, rev)
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
        self._set_upstream()

    def pull_to(self, path, to_info):
        try:
            out = None
            with suppress(OutputNotFoundError):
                out = self.find_out_by_relpath(path)

            if out and out.use_cache:
                self._pull_cached(out, to_info)
                return

            # Git handled files can't have absolute path
            if os.path.isabs(path):
                raise FileNotFoundError

            fs_copy(os.path.join(self.root_dir, path), fspath(to_info))
        except FileNotFoundError:
            raise PathMissingError(path, self.url)

    def _pull_cached(self, out, to_info):
        with self.state:
            # Only pull unless all needed cache is present
            if out.changed_cache():
                self.cloud.pull(out.get_used_cache())

            out.path_info = to_info
            failed = out.checkout()
            # This might happen when pull haven't really pulled all the files
            if failed:
                raise FileNotFoundError

    @wrap_with(threading.Lock())
    def _set_cache_dir(self):
        try:
            cache_dir = CACHE_DIRS[self.url]
        except KeyError:
            cache_dir = CACHE_DIRS[self.url] = tempfile.mkdtemp("dvc-cache")

        self.cache.local.cache_dir = cache_dir

    def _set_upstream(self):
        # check if the URL is local and no default remote is present
        # add default remote pointing to the original repo's cache location
        if os.path.isdir(self.url):
            rconfig = RemoteConfig(self.config)
            if not rconfig.has_default():
                src_repo = Repo(self.url)
                try:
                    rconfig.add(
                        "auto-generated-upstream",
                        src_repo.cache.local.cache_dir,
                        default=True,
                        level=Config.LEVEL_LOCAL,
                    )
                finally:
                    src_repo.close()


class ExternalGitRepo:
    def __init__(self, root_dir, url):
        self.root_dir = root_dir
        self.url = url

    @cached_property
    def scm(self):
        return SCM(self.root_dir)

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


@wrap_with(threading.Lock())
def _cached_clone(url, rev):
    """Clone an external git repo to a temporary directory.

    Returns the path to a local temporary directory with the specified
    revision checked out.
    """
    # Get or create a clean clone
    clone_path = CLONES.get(url)
    if clone_path:
        # Do not pull for known shas, branches and tags might move
        if not _is_known_sha(clone_path, rev):
            _git_pull(clone_path)
    else:
        clone_path = tempfile.mkdtemp("dvc-clone")
        _git_clone(url, clone_path)
        CLONES[url] = clone_path

    # Copy to a new dir to keep the clone clean
    repo_path = tempfile.mkdtemp("dvc-erepo")
    copy_tree(clone_path, repo_path)

    # Check out the specified revision
    if rev is not None:
        _git_checkout(repo_path, rev)

    return repo_path


def _git_clone(url, path):
    from dvc.scm.git import Git

    git = Git.clone(url, path)
    git.close()


def _git_checkout(repo_path, rev):
    from dvc.scm import Git

    git = Git(repo_path)
    try:
        git.checkout(rev)
    finally:
        git.close()


def _is_known_sha(repo_path, rev):
    import git
    from gitdb.exc import BadName

    if not rev or not git.Repo.re_hexsha_shortened.search(rev):
        return False

    try:
        git.Repo(repo_path).commit(rev)
        return True
    except BadName:
        return False


def _git_pull(repo_path):
    import git

    repo = git.Repo(repo_path)
    try:
        msg = repo.git.pull()
        logger.debug("external repo: git pull: {}", msg)
    finally:
        repo.close()


def _remove(path):
    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        os_retry(remove)(path)
    else:
        remove(path)
