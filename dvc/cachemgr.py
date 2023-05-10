import os

from dvc.fs import GitFileSystem, Schemes
from dvc_data.hashfile.db import get_odb


def _get_odb(repo, settings, fs=None):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    cls, config, fs_path = get_cloud_fs(repo, **settings)
    fs = fs or cls(**config)
    return get_odb(fs, fs_path, state=repo.state, **config)


class CacheManager:
    CACHE_DIR = "cache"
    CLOUD_SCHEMES = [
        Schemes.S3,
        Schemes.GS,
        Schemes.SSH,
        Schemes.HDFS,
        Schemes.WEBHDFS,
    ]

    def __init__(self, repo):
        self._repo = repo
        self.config = config = repo.config["cache"]
        self._odb = {}

        default = None
        if repo and repo.local_dvc_dir:
            default = os.path.join(repo.local_dvc_dir, self.CACHE_DIR)

        local = config.get("local")

        if local:
            settings = {"name": local}
        elif "dir" not in config and not default:
            settings = None
        else:
            from dvc.config_schema import LOCAL_COMMON

            url = config.get("dir") or default
            settings = {"url": url}
            for opt in LOCAL_COMMON:
                if opt in config:
                    settings[str(opt)] = config.get(opt)

        kwargs = {}
        if not isinstance(repo.fs, GitFileSystem):
            kwargs["fs"] = repo.fs

        odb = _get_odb(repo, settings, **kwargs)
        self._odb["repo"] = odb
        self._odb[Schemes.LOCAL] = odb

    def _init_odb(self, schemes):
        for scheme in schemes:
            remote = self.config.get(scheme)
            settings = {"name": remote} if remote else None
            self._odb[scheme] = _get_odb(self._repo, settings)

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
