from collections import defaultdict

from dvc.scheme import Schemes


def get_odb(fs):
    from .base import ObjectDB
    from .gdrive import GDriveObjectDB
    from .local import LocalObjectDB
    from .ssh import SSHObjectDB

    if fs.scheme == Schemes.LOCAL:
        return LocalObjectDB(fs)

    if fs.scheme == Schemes.SSH:
        return SSHObjectDB(fs)

    if fs.scheme == Schemes.GDRIVE:
        return GDriveObjectDB(fs)

    return ObjectDB(fs)


def _get_odb(repo, settings):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    fs = get_cloud_fs(repo, **settings)
    return get_odb(fs)


class ODBManager:
    CACHE_DIR = "cache"
    CLOUD_SCHEMES = [
        Schemes.S3,
        Schemes.GS,
        Schemes.SSH,
        Schemes.HDFS,
        Schemes.WEBHDFS,
    ]

    def __init__(self, repo):
        self.repo = repo
        self.config = config = repo.config["cache"]
        self._odb = {}

        local = config.get("local")

        if local:
            settings = {"name": local}
        elif "dir" not in config:
            settings = None
        else:
            from dvc.config_schema import LOCAL_COMMON

            settings = {"url": config["dir"]}
            for opt in LOCAL_COMMON.keys():
                if opt in config:
                    settings[str(opt)] = config.get(opt)

        self._odb[Schemes.LOCAL] = _get_odb(repo, settings)

    def _init_odb(self, schemes):
        for scheme in schemes:
            remote = self.config.get(scheme)
            settings = {"name": remote} if remote else None
            self._odb[scheme] = _get_odb(self.repo, settings)

    def __getattr__(self, name):
        if name not in self._odb and name in self.CLOUD_SCHEMES:
            self._init_odb([name])

        try:
            return self._odb[name]
        except KeyError as exc:
            raise AttributeError from exc

    def by_scheme(self):
        self._init_odb(self.CLOUD_SCHEMES)
        yield from self._odb.items()


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
