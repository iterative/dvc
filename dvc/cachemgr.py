import os
from typing import TYPE_CHECKING, Optional

from dvc.fs import GitFileSystem, Schemes
from dvc_data.hashfile.db import get_odb
from dvc_data.hashfile.hash import DEFAULT_ALGORITHM

if TYPE_CHECKING:
    from dvc.repo import Repo

LEGACY_HASH_NAMES = {"md5-dos2unix", "params"}


def _get_odb(
    repo,
    settings,
    fs=None,
    prefix: Optional[tuple[str, ...]] = None,
    hash_name: Optional[str] = None,
    **kwargs,
):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    cls, config, fs_path = get_cloud_fs(repo.config, **settings)
    fs = fs or cls(**config)
    if prefix:
        fs_path = fs.join(fs_path, *prefix)
    if hash_name:
        config["hash_name"] = hash_name
    return get_odb(fs, fs_path, state=repo.state, **config)


class CacheManager:
    CACHE_DIR = "cache"
    FILES_DIR = "files"
    FS_DIR = "fs"

    def __init__(self, repo):
        self._repo = repo
        self.config = config = repo.config["cache"]
        self._odb = {}

        local = config.get("local")
        default = self.default_local_cache_dir

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

    @property
    def fs_cache(self):
        """Filesystem-based cache.

        Currently used as a temporary location to download files that we don't
        yet have a regular oid (e.g. md5) for.
        """
        from dvc_data.index import FileStorage

        return FileStorage(
            key=(),
            fs=self.local.fs,
            path=self.local.fs.join(self.default_local_cache_dir, self.FS_DIR),
        )

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
        try:
            return self._odb[name]
        except KeyError as exc:
            raise AttributeError from exc

    def by_scheme(self):
        yield from self._odb.items()

    @property
    def local_cache_dir(self) -> str:
        """Return base local cache directory without any prefixes.

        (i.e. `dvc cache dir`).
        """
        return self.legacy.path

    @property
    def default_local_cache_dir(self) -> Optional[str]:
        repo = self._repo
        if repo and repo.local_dvc_dir:
            return os.path.join(repo.local_dvc_dir, self.CACHE_DIR)
        return None


def migrate_2_to_3(repo: "Repo", dry: bool = False):
    """Migrate legacy 2.x objects to 3.x cache.

    Legacy 'md5-dos2unix' objects will be re-hashed with 'md5', added to 3.x cache,
    and then a link from the legacy 2.x location to the 3.x location will be created.
    """
    from dvc.fs.callbacks import TqdmCallback
    from dvc.ui import ui
    from dvc_data.hashfile.db.migrate import migrate, prepare

    src = repo.cache.legacy
    dest = repo.cache.local
    if dry:
        oids = list(src._list_oids())
        ui.write(
            f"{len(oids)} files will be re-hashed and migrated to the DVC 3.0 cache "
            "location."
        )
        return

    with TqdmCallback(desc="Computing DVC 3.0 hashes", unit="files") as cb:
        migration = prepare(src, dest, callback=cb)

    with TqdmCallback(desc="Migrating to DVC 3.0 cache", unit="files") as cb:
        count = migrate(migration, callback=cb)
    ui.write(f"Migrated {count} files to DVC 3.0 cache location.")
