"""Manages cache of a DVC repo."""
from collections import defaultdict

from funcy import cached_property


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
        self.config = config = repo.config["cache"]

        local = config.get("local")

        if local:
            settings = {"name": local}
        else:
            settings = {**config, "url": config["dir"]}

        self.local = Remote(repo, **settings)

    s3 = _make_remote_property("s3")
    gs = _make_remote_property("gs")
    ssh = _make_remote_property("ssh")
    hdfs = _make_remote_property("hdfs")
    azure = _make_remote_property("azure")


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
