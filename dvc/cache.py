"""Manages cache of a DVC repo."""
import os
from collections import defaultdict

from funcy import cached_property

from dvc.config import Config


class CacheConfig(object):
    def __init__(self, config):
        self.config = config

    def set_dir(self, dname, level=None):
        from dvc.remote.config import RemoteConfig

        configobj = self.config.get_configobj(level)
        path = RemoteConfig.resolve_path(dname, configobj.filename)
        self.config.set(
            Config.SECTION_CACHE, Config.SECTION_CACHE_DIR, path, level=level
        )


def _make_remote_property(name):
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

    This method creates a cached property, containing cache named `name`:

        self.config == {'ssh': 'dvc-storage'}
        self.ssh  # a RemoteSSH instance
    """

    def getter(self):
        from dvc.remote import Remote

        remote = self.config.get(name)
        if not remote:
            return None

        return Remote(self.repo, name=remote)

    getter.__name__ = name
    return cached_property(getter)


class Cache(object):
    """Class that manages cache locations of a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that this cache belongs to.
    """

    CACHE_DIR = "cache"

    def __init__(self, repo):
        from dvc.remote import Remote

        self.repo = repo

        self.config = config = repo.config.config[Config.SECTION_CACHE]
        local = config.get(Config.SECTION_CACHE_LOCAL)

        if local:
            name = Config.SECTION_REMOTE_FMT.format(local)
            settings = repo.config.config[name]
        else:
            default_cache_dir = os.path.join(repo.dvc_dir, self.CACHE_DIR)
            cache_dir = config.get(Config.SECTION_CACHE_DIR, default_cache_dir)
            cache_type = config.get(Config.SECTION_CACHE_TYPE)
            protected = config.get(Config.SECTION_CACHE_PROTECTED)
            shared = config.get(Config.SECTION_CACHE_SHARED)

            settings = {
                Config.PRIVATE_CWD: config.get(
                    Config.PRIVATE_CWD, repo.dvc_dir
                ),
                Config.SECTION_REMOTE_URL: cache_dir,
                Config.SECTION_CACHE_TYPE: cache_type,
                Config.SECTION_CACHE_PROTECTED: protected,
                Config.SECTION_CACHE_SHARED: shared,
            }

        self.local = Remote(repo, **settings)

    s3 = _make_remote_property(Config.SECTION_CACHE_S3)
    gs = _make_remote_property(Config.SECTION_CACHE_GS)
    ssh = _make_remote_property(Config.SECTION_CACHE_SSH)
    hdfs = _make_remote_property(Config.SECTION_CACHE_HDFS)
    azure = _make_remote_property(Config.SECTION_CACHE_AZURE)


class NamedCache(object):
    def __init__(self):
        self._items = defaultdict(lambda: defaultdict(set))
        self.external = defaultdict(set)

    @classmethod
    def make(cls, scheme, checksum, name):
        cache = cls()
        cache.add(scheme, checksum, name)
        return cache

    def __getitem__(self, key):
        return self._items[key]

    def add(self, scheme, checksum, name):
        self._items[scheme][checksum].add(name)

    def add_external(self, url, rev, path):
        self.external[url, rev].add(path)

    def update(self, cache, suffix=""):
        for scheme, src in cache._items.items():
            dst = self._items[scheme]
            for checksum, names in src.items():
                if suffix:
                    dst[checksum].update(n + suffix for n in names)
                else:
                    dst[checksum].update(names)

        for repo_pair, files in cache.external.items():
            self.external[repo_pair].update(files)
