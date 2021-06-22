import os

from dvc.scheme import Schemes


def get_odb(fs, path_info, **config):
    from .base import ObjectDB
    from .gdrive import GDriveObjectDB
    from .local import LocalObjectDB
    from .ssh import SSHObjectDB

    if fs.scheme == Schemes.LOCAL:
        return LocalObjectDB(fs, path_info, **config)

    if fs.scheme == Schemes.SSH:
        return SSHObjectDB(fs, path_info, **config)

    if fs.scheme == Schemes.GDRIVE:
        return GDriveObjectDB(fs, path_info, **config)

    return ObjectDB(fs, path_info, **config)


def _get_odb(repo, settings, staging=None):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    cls, config, path_info = get_cloud_fs(repo, **settings)
    return get_odb(
        cls(**config), path_info, state=repo.state, staging=staging, **config
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
    STAGING_PATH = "dvc-staging"
    STAGING_MEMFS_URL = f"memory://{STAGING_PATH}"

    def __init__(self, repo):
        from dvc.fs.memory import MemoryFileSystem
        from dvc.path_info import CloudURLInfo

        from .base import ObjectDB

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

        if settings is not None and self.repo.tmp_dir is not None:
            staging_settings = dict(settings)
            staging_settings["url"] = os.path.join(
                self.repo.tmp_dir, self.STAGING_PATH
            )
            staging = _get_odb(repo, staging_settings)
        else:
            staging = None

        self._odb[Schemes.LOCAL] = _get_odb(repo, settings, staging=staging)
        self._odb[Schemes.MEMORY] = ObjectDB(
            MemoryFileSystem(), CloudURLInfo(self.STAGING_MEMFS_URL)
        )

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
        for scheme in self.CLOUD_SCHEMES:
            yield scheme, self._odb[scheme]
