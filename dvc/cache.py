"""Manages cache of a dvc repo."""

from __future__ import unicode_literals

import os

from dvc.config import Config


class Cache(object):
    """Class that manages cache locations of a dvc repo.

    Args:
        repo (dvc.repo.Repo): repo instance that this cache belongs to.
    """

    CACHE_DIR = "cache"

    def __init__(self, repo):
        from dvc.remote import Remote

        self.repo = repo

        config = repo.config.config[Config.SECTION_CACHE]
        local = config.get(Config.SECTION_CACHE_LOCAL)

        if local:
            name = Config.SECTION_REMOTE_FMT.format(local)
            sect = repo.config.config[name]
        else:
            default_cache_dir = os.path.join(repo.dvc_dir, self.CACHE_DIR)
            cache_dir = config.get(Config.SECTION_CACHE_DIR, default_cache_dir)
            cache_type = config.get(Config.SECTION_CACHE_TYPE)
            protected = config.get(Config.SECTION_CACHE_PROTECTED)

            sect = {
                Config.PRIVATE_CWD: config.get(
                    Config.PRIVATE_CWD, repo.dvc_dir
                ),
                Config.SECTION_REMOTE_URL: cache_dir,
                Config.SECTION_CACHE_TYPE: cache_type,
                Config.SECTION_CACHE_PROTECTED: protected,
            }

        self._local = Remote(repo, sect)

        self._s3 = self._get_remote(config, Config.SECTION_CACHE_S3)
        self._gs = self._get_remote(config, Config.SECTION_CACHE_GS)
        self._ssh = self._get_remote(config, Config.SECTION_CACHE_SSH)
        self._hdfs = self._get_remote(config, Config.SECTION_CACHE_HDFS)
        self._azure = self._get_remote(config, Config.SECTION_CACHE_AZURE)

    @property
    def local(self):
        """Remote instance for local cache."""
        return self._local

    @property
    def s3(self):
        """Remote instance for AWS S3 cache."""
        return self._s3

    @property
    def gs(self):
        """Remote instance for Google Cloud Storage cache."""
        return self._gs

    @property
    def ssh(self):
        """Remote instance for SSH cache."""
        return self._ssh

    @property
    def hdfs(self):
        """Remote instance for HDFS cache."""
        return self._hdfs

    @property
    def azure(self):
        """Remote instance for azure cache."""
        return self._azure

    def _get_remote(self, config, name):
        from dvc.remote import Remote

        remote = config.get(name, None)
        if not remote:
            return None

        name = Config.SECTION_REMOTE_FMT.format(remote)
        sect = self.repo.config.config[name]
        return Remote(self.repo, sect)
