from __future__ import unicode_literals

import os
import tempfile
from contextlib import contextmanager
from distutils.dir_util import copy_tree

from dvc.remote import RemoteConfig
from funcy import retry

from dvc.config import NoRemoteError, ConfigError
from dvc.exceptions import RemoteNotSpecifiedInExternalRepoError
from dvc.exceptions import NoOutputInExternalRepoError
from dvc.exceptions import OutputNotFoundError
from dvc.utils.fs import remove


REPO_CACHE = {}


@contextmanager
def external_repo(url=None, rev=None, rev_lock=None, cache_dir=None):
    from dvc.repo import Repo

    path = _external_repo(url=url, rev=rev_lock or rev, cache_dir=cache_dir)
    repo = Repo(path)
    try:
        yield repo
    except NoRemoteError as exc:
        raise RemoteNotSpecifiedInExternalRepoError(url, cause=exc)
    except OutputNotFoundError as exc:
        if exc.repo is repo:
            raise NoOutputInExternalRepoError(exc.output, repo.root_dir, url)
        raise
    repo.close()


def _external_repo(url=None, rev=None, cache_dir=None):
    from dvc.config import Config
    from dvc.cache import CacheConfig
    from dvc.repo import Repo

    key = (url, rev, cache_dir)
    if key in REPO_CACHE:
        return REPO_CACHE[key]

    new_path = tempfile.mkdtemp("dvc-erepo")

    # Copy and adjust existing clone
    if (url, None, None) in REPO_CACHE:
        old_path = REPO_CACHE[url, None, None]

        # This one unlike shutil.copytree() works with an existing dir
        copy_tree(old_path, new_path)
    else:
        # Create a new clone
        _clone_repo(url, new_path)

        # Save clean clone dir so that we will have access to a default branch
        clean_clone_path = tempfile.mkdtemp("dvc-erepo")
        copy_tree(new_path, clean_clone_path)
        REPO_CACHE[url, None, None] = clean_clone_path

    # Adjust new clone/copy to fit rev and cache_dir
    repo = Repo(new_path)
    # Adjust original repo for pointing remote towards its' cache
    original_repo = Repo(url)
    rconfig = RemoteConfig(original_repo.config)
    try:
        if rev is not None:
            repo.scm.checkout(rev)

        if not _is_local(url) and not _remote_config_exists(rconfig):
            # check if the URL is local and no default remote
            # add default remote pointing to the original repo's cache location
            rconfig.add("upstream",
                        original_repo.cache.local.cache_dir,
                        default=True)
            original_repo.scm.add([original_repo.config.config_file])
            original_repo.scm.commit("add remote")

        if cache_dir is not None:
            cache_config = CacheConfig(repo.config)
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)
    finally:
        # Need to close/reopen repo to force config reread
        repo.close()
        original_repo.close()

    REPO_CACHE[key] = new_path
    return new_path


def clean_repos():
    # Outside code should not see cache while we are removing
    repo_paths = list(REPO_CACHE.values())
    REPO_CACHE.clear()

    for path in repo_paths:
        _remove(path)


def _remove(path):
    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        os_retry(remove)(path)
    else:
        remove(path)


def _clone_repo(url, path):
    from dvc.scm.git import Git

    git = Git.clone(url, path)
    git.close()


def _remote_config_exists(rconfig):
    """
    Checks if default remote config is present.
    Args:
        rconfig: a remote config

    Returns:
        True if the remote config exists, else False
    """
    try:
        default = rconfig.get_default()
    except ConfigError:
        default = None
    return True if default else False


def _is_local(url):
    """
    Checks if the URL is local or not.
    Args:
        url: url

    Returns:
        True, if the URL is local else False
    """
    remote_urls = {"azure://", "gs://", "http://", "https://",
                   "oss://", "s3://", "hdfs://"}
    for remote_url in remote_urls:
        if url.startswith(remote_url):
            return False
    return True
