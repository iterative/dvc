import logging

from .base import ObjectDB

logger = logging.getLogger(__name__)


class GitObjectDB(ObjectDB):
    """Dummy read-only ODB for uncached objects in external Git repos."""

    def __init__(self, fs, path_info, **config):
        from dvc.fs.repo import RepoFileSystem

        assert isinstance(fs, RepoFileSystem)
        super().__init__(fs, path_info)

    def get(self, hash_info):
        raise NotImplementedError

    def add(self, path_info, fs, hash_info, move=True, **kwargs):
        raise NotImplementedError

    def gc(self, used, jobs=None):
        raise NotImplementedError
