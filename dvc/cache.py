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
            settings = repo.config.config[name]
        else:
            default_cache_dir = os.path.join(repo.dvc_dir, self.CACHE_DIR)
            cache_dir = config.get(Config.SECTION_CACHE_DIR, default_cache_dir)
            cache_type = config.get(Config.SECTION_CACHE_TYPE)
            protected = config.get(Config.SECTION_CACHE_PROTECTED)

            settings = {
                Config.PRIVATE_CWD: config.get(
                    Config.PRIVATE_CWD, repo.dvc_dir
                ),
                Config.SECTION_REMOTE_URL: cache_dir,
                Config.SECTION_CACHE_TYPE: cache_type,
                Config.SECTION_CACHE_PROTECTED: protected,
            }

        self.local = Remote(repo, settings)
        self.s3 = self._get_remote(config, Config.SECTION_CACHE_S3)
        self.gs = self._get_remote(config, Config.SECTION_CACHE_GS)
        self.ssh = self._get_remote(config, Config.SECTION_CACHE_SSH)
        self.hdfs = self._get_remote(config, Config.SECTION_CACHE_HDFS)
        self.azure = self._get_remote(config, Config.SECTION_CACHE_AZURE)

    def _get_remote(self, config, name):
        """
        The config file is stored in a way that allows you to have a
        cache for each remote.

        This is needed when specifying external outputs
        (as they require you to have an external cache location).

        Imagine a config file like the following:

                ['remote "dvc-storage"']
                url = ssh://localhost/tmp
                ask_password = true

                [cache]
                ssh = dvc-storage

        This method resolves the name under the cache section into the
        correct Remote instance.

        Args:
            config (dict): The cache section on the config file
            name (str): Name of the section we are interested in to retrieve

        Returns:
            remote (dvc.Remote): Remote instance that the section is referring.
                None when there's no remote with that name.

        Example:
            >>> _get_remote(config={'ssh': 'dvc-storage'}, name='ssh')
        """
        from dvc.remote import Remote

        remote = config.get(name)

        if not remote:
            return None

        settings = self.repo.config.get_remote_settings(remote)
        return Remote(self.repo, settings)
