from typing import TYPE_CHECKING

from dvc.scheme import Schemes

if TYPE_CHECKING:
    from .index import ObjectDBIndexBase


def get_odb(fs, path_info, **config):
    from .base import ObjectDB
    from .gdrive import GDriveObjectDB
    from .local import LocalObjectDB
    from .oss import OSSObjectDB

    if fs.scheme == Schemes.LOCAL:
        return LocalObjectDB(fs, path_info, **config)

    if fs.scheme == Schemes.GDRIVE:
        return GDriveObjectDB(fs, path_info, **config)

    if fs.scheme == Schemes.OSS:
        return OSSObjectDB(fs, path_info, **config)

    return ObjectDB(fs, path_info, **config)


def _get_odb(repo, settings):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    cls, config, path_info = get_cloud_fs(repo, **settings)
    config["tmp_dir"] = repo.tmp_dir
    return get_odb(cls(**config), path_info, state=repo.state, **config)


def get_index(odb) -> "ObjectDBIndexBase":
    import hashlib

    from .index import ObjectDBIndex, ObjectDBIndexNoop

    cls = ObjectDBIndex if odb.tmp_dir else ObjectDBIndexNoop
    return cls(
        odb.tmp_dir,
        hashlib.sha256(odb.path_info.url.encode("utf-8")).hexdigest(),
        odb.fs.CHECKSUM_DIR_SUFFIX,
    )


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
