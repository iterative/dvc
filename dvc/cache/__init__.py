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
        from ..tree import get_cloud_tree
        from .base import CloudCache

        remote = self.config.get(name)
        if not remote:
            return None

        tree = get_cloud_tree(self.repo, name=remote)
        return CloudCache(tree)

    getter.__name__ = name
    return cached_property(getter)


class Cache:
    """Class that manages cache locations of a DVC repo.

    Args:
        repo (dvc.repo.Repo): repo instance that this cache belongs to.
    """

    CACHE_DIR = "cache"

    def __init__(self, repo):
        from ..tree import get_cloud_tree
        from .local import LocalCache

        self.repo = repo
        self.config = config = repo.config["cache"]

        local = config.get("local")

        if local:
            settings = {"name": local}
        else:
            settings = {**config, "url": config["dir"]}

        tree = get_cloud_tree(repo, **settings)
        self.local = LocalCache(tree)

    s3 = _make_remote_property("s3")
    gs = _make_remote_property("gs")
    ssh = _make_remote_property("ssh")
    hdfs = _make_remote_property("hdfs")
    azure = _make_remote_property("azure")


class NamedCacheItem:
    def __init__(self):
        self.names = set()
        self.children = defaultdict(NamedCacheItem)

    def __eq__(self, other):
        return self.names == other.names and self.children == other.children

    def child_keys(self):
        for key, child in self.children.items():
            yield key
            yield from child.child_keys()

    def child_names(self):
        for key, child in self.children.items():
            yield key, child.names
            yield from child.child_names()

    def add(self, checksum, item):
        self.children[checksum].update(item)

    def update(self, item, suffix=""):
        if suffix:
            self.names.update(n + suffix for n in item.names)
        else:
            self.names.update(item.names)
        for checksum, child_item in item.children.items():
            self.children[checksum].update(child_item)


class NamedCache:
    # pylint: disable=protected-access
    def __init__(self):
        self._items = defaultdict(lambda: defaultdict(NamedCacheItem))
        self.external = defaultdict(set)

    @classmethod
    def make(cls, scheme, checksum, name):
        cache = cls()
        cache.add(scheme, checksum, name)
        return cache

    def __getitem__(self, key):
        return self._items[key]

    def add(self, scheme, checksum, name):
        """Add a mapped name for the specified checksum."""
        self._items[scheme][checksum].names.add(name)

    def add_child_cache(self, checksum, cache, suffix=""):
        """Add/update child cache for the specified checksum."""
        for scheme, src in cache._items.items():
            dst = self._items[scheme][checksum].children
            for child_checksum, item in src.items():
                dst[child_checksum].update(item, suffix=suffix)

        for repo_pair, files in cache.external.items():
            self.external[repo_pair].update(files)

    def add_external(self, url, rev, path):
        self.external[url, rev].add(path)

    def update(self, cache, suffix=""):
        for scheme, src in cache._items.items():
            dst = self._items[scheme]
            for checksum, item in src.items():
                dst[checksum].update(item, suffix=suffix)

        for repo_pair, files in cache.external.items():
            self.external[repo_pair].update(files)

    def scheme_keys(self, scheme):
        """Iterate over a flat list of all keys for the specified scheme,
        including children.
        """
        for key, item in self._items[scheme].items():
            yield key
            yield from item.child_keys()

    def scheme_names(self, scheme):
        """Iterate over a flat list of checksum, names items for the specified
        scheme, including children.
        """
        for key, item in self._items[scheme].items():
            yield key, item.names
            yield from item.child_names()

    def dir_keys(self, scheme):
        return (
            key for key, item in self._items[scheme].items() if item.children
        )

    def child_keys(self, scheme, checksum):
        return self._items[scheme][checksum].child_keys()
