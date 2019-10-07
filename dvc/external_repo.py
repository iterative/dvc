from __future__ import unicode_literals

import os
import tempfile
from distutils.dir_util import copy_tree

from funcy import retry
from contextlib import contextmanager

from dvc.utils import remove


REPO_CACHE = {}
REPO_BY_URL = {}


@contextmanager
def external_repo(url=None, rev=None, rev_lock=None, cache_dir=None):
    from dvc.repo import Repo

    path = _external_repo(url=url, rev=rev_lock or rev, cache_dir=cache_dir)
    repo = Repo(path)
    yield repo
    repo.close()


def _external_repo(url=None, rev=None, cache_dir=None):
    key = (url, rev, cache_dir)
    if key in REPO_CACHE:
        return REPO_CACHE[key]

    new_path = tempfile.mkdtemp("dvc-erepo")

    # Copy and adjust existing clone
    if url in REPO_BY_URL:
        old_path, old_rev, old_cache_dir = REPO_BY_URL[url]

        # This one unlike shutil.copytree() works with an existing dir
        copy_tree(old_path, new_path)

        if old_rev != rev:
            _set_rev(new_path, rev)

        if old_cache_dir != cache_dir:
            _set_cache_dir(new_path, cache_dir)

        REPO_CACHE[key] = new_path
        return new_path

    # Create a new clone
    _clone_repo(url, new_path, rev=rev)
    if cache_dir:
        _set_cache_dir(new_path, cache_dir)

    REPO_CACHE[key] = new_path
    REPO_BY_URL[url] = new_path, rev, cache_dir
    return new_path


def clean_repos():
    # Outside code should not see these while we are removing
    repo_paths = list(REPO_CACHE.values())
    REPO_CACHE.clear()
    REPO_BY_URL.clear()

    for path in repo_paths:
        _remove(path)


def _remove(path):
    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        os_retry(remove)(path)
    else:
        remove(path)


def _clone_repo(url, path, rev=None):
    from dvc.scm.git import Git

    git = Git.clone(url, path, rev=rev)
    git.close()


def _set_rev(path, rev):
    from dvc.repo import Repo

    repo = Repo(path)

    try:
        repo.scm.checkout(rev or "master")
    finally:
        repo.close()


def _set_cache_dir(path, cache_dir):
    from dvc.config import Config
    from dvc.cache import CacheConfig
    from dvc.repo import Repo

    repo = Repo(path)

    try:
        cache_config = CacheConfig(repo.config)
        if cache_dir:
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)
        else:
            cache_config.unset_dir(level=Config.LEVEL_LOCAL)
    finally:
        repo.close()
