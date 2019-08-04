from __future__ import unicode_literals

import os
import logging
import tempfile

from funcy import retry
from contextlib import contextmanager

from dvc.config import Config
from dvc.cache import CacheConfig
from dvc.exceptions import DvcException
from dvc.utils import remove


logger = logging.getLogger(__name__)


class ExternalRepoError(DvcException):
    pass


class CloneError(ExternalRepoError):
    def __init__(self, url, path, cause):
        super(CloneError, self).__init__(
            "Failed to clone repo '{}' to '{}'".format(url, path), cause=cause
        )


class RevError(ExternalRepoError):
    def __init__(self, url, rev, cause):
        super(RevError, self).__init__(
            "Failed to access revision '{}' for repo '{}'".format(rev, url),
            cause=cause,
        )


def _clone(cache_dir=None, **kwargs):
    from dvc.repo import Repo

    _path = tempfile.mkdtemp("dvc-repo")

    _clone_git_repo(_path, **kwargs)

    if cache_dir:
        repo = Repo(_path)
        cache_config = CacheConfig(repo.config)
        cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)
        repo.scm.close()

    return Repo(_path)


def _clone_git_repo(to_path, url=None, rev=None, rev_lock=None):
    import git

    try:
        repo = git.Repo.clone_from(url, to_path, no_single_branch=True)
    except git.exc.GitCommandError as exc:
        raise CloneError(url, to_path, exc)
    try:
        revision = rev_lock or rev
        if revision:
            try:
                repo.git.checkout(revision)
            except git.exc.GitCommandError as exc:
                raise RevError(url, revision, exc)
    finally:
        repo.close()


def _remove(repo):
    repo.scm.close()

    if os.name == "nt":
        # git.exe may hang for a while not permitting to remove temp dir
        os_retry = retry(5, errors=OSError, timeout=0.1)
        os_retry(remove)(repo.root_dir)
    else:
        remove(repo.root_dir)


@contextmanager
def external_repo(**kwargs):
    repo = _clone(**kwargs)
    yield repo
    _remove(repo)
