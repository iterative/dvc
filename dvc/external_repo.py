from __future__ import unicode_literals

import os
import tempfile
from distutils.dir_util import copy_tree

from contextlib import contextmanager
from funcy import retry

from dvc.utils import remove


REPO_CACHE = {}


@contextmanager
def external_repo(url=None, rev=None, rev_lock=None, cache_dir=None):
    from dvc.repo import Repo

    path = _external_repo(url=url, rev=rev_lock or rev, cache_dir=cache_dir)
    repo = Repo(path)
    yield repo
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
    try:
        if rev is not None:
            repo.scm.checkout(rev)

        if cache_dir is not None:
            cache_config = CacheConfig(repo.config)
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)
    finally:
        # Need to close/reopen repo to force config reread
        repo.close()

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
