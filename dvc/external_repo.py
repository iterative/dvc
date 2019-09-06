from __future__ import unicode_literals

import os
import logging
import tempfile

from funcy import retry
from contextlib import contextmanager

from dvc.config import Config
from dvc.cache import CacheConfig
from dvc.utils import remove


logger = logging.getLogger(__name__)


def _clone(cache_dir=None, url=None, rev=None, rev_lock=None):
    from dvc.repo import Repo

    _path = tempfile.mkdtemp("dvc-repo")

    repo = Repo.clone(url, _path, rev=(rev_lock or rev))

    try:
        if cache_dir:
            cache_config = CacheConfig(repo.config)
            cache_config.set_dir(cache_dir, level=Config.LEVEL_LOCAL)
    finally:
        repo.close()

    return Repo(_path)


def _remove(repo):
    repo.close()

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
