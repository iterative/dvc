import os
from typing import Optional, Tuple

from dvc.fs import GitFileSystem, Schemes
from dvc_data.hashfile.db import get_odb
from dvc_data.hashfile.hash import DEFAULT_ALGORITHM

LEGACY_HASH_NAMES = {"md5-dos2unix", "params"}


def _get_odb(
    repo,
    settings,
    fs=None,
    prefix: Optional[Tuple[str, ...]] = None,
    hash_name: Optional[str] = None,
    **kwargs,
):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    cls, config, fs_path = get_cloud_fs(repo, **settings)
    fs = fs or cls(**config)
    if prefix:
        fs_path = fs.path.join(fs_path, *prefix)
    if hash_name:
        config["hash_name"] = hash_name
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
    FILES_DIR = "files"

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

        odb = _get_odb(
            repo,
            settings,
            prefix=(self.FILES_DIR, DEFAULT_ALGORITHM),
            **kwargs,
        )
        self._odb["repo"] = odb
        self._odb[Schemes.LOCAL] = odb
        legacy_odb = _get_odb(repo, settings, hash_name="md5-dos2unix", **kwargs)
        self._odb["legacy"] = legacy_odb

    def _init_odb(self, schemes):
        for scheme in schemes:
            remote = self.config.get(scheme)
            settings = {"name": remote} if remote else None
            self._odb[scheme] = _get_odb(
                self._repo,
                settings,
                prefix=(self.FILES_DIR, DEFAULT_ALGORITHM),
            )

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

    @property
    def local_cache_dir(self) -> str:
        """Return base local cache directory without any prefixes.

        (i.e. `dvc cache dir`).
        """
        return self.legacy.path
