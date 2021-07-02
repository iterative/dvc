import os
from typing import TYPE_CHECKING, Optional

from dvc.scheme import Schemes

if TYPE_CHECKING:
    from dvc.fs.base import BaseFileSystem
    from dvc.types import DvcPath

    from ..file import HashFile
    from .base import ObjectDB


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


def _get_odb(repo, settings):
    from dvc.fs import get_cloud_fs

    if not settings:
        return None

    cls, config, path_info = get_cloud_fs(repo, **settings)
    return get_odb(cls(**config), path_info, state=repo.state, **config)


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

    def __init__(self, repo):
        from shortuuid import uuid

        from dvc.fs.memory import MemoryFileSystem
        from dvc.path_info import CloudURLInfo

        from .base import ObjectDB

        self.repo = repo
        self.config = config = repo.config["cache"]
        self._odb = {}
        self._staging = {}

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

        if (
            settings is not None
            and "name" not in settings
            and self.repo.tmp_dir is not None
        ):
            staging_settings = dict(settings)
            staging_settings["url"] = os.path.join(
                self.repo.tmp_dir, self.STAGING_PATH
            )
            self._staging[Schemes.LOCAL] = _get_odb(repo, staging_settings)

        self._odb[Schemes.LOCAL] = _get_odb(repo, settings)

        # NOTE: generate unique memfs URL per ODB manager - since memfs is
        # global, non-unique URL will cause concurrency issues between
        # Repo() instances
        memfs_url = CloudURLInfo(f"memory://{self.STAGING_PATH}-{uuid()}")
        self._staging[Schemes.MEMORY] = ObjectDB(MemoryFileSystem(), memfs_url)

    @property
    def state(self):
        return self.repo.state

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
        for scheme in [Schemes.LOCAL] + self.CLOUD_SCHEMES:
            yield scheme, self._odb[scheme]

    def get_staging(
        self, scheme: Optional[str] = None
    ) -> Optional["ObjectDB"]:
        return self._staging.get(scheme or Schemes.MEMORY)

    def load_from_state(
        self,
        scheme: str,
        path_info: "DvcPath",
        fs: "BaseFileSystem",
        name: str,
    ) -> "HashFile":
        from .. import load
        from ..errors import ObjectFormatError

        main_odb = self._odb.get(scheme)
        staging_odb = self._staging.get(scheme)

        hash_info = self.state.get(path_info, fs)
        if hash_info:
            for odb in (staging_odb, main_odb):
                if odb and odb.exists(hash_info):
                    try:
                        obj = load(odb, hash_info)
                        self._fixup_state_obj(
                            obj, hash_info, fs, path_info, name
                        )
                        return obj
                    except ObjectFormatError:
                        pass
        raise FileNotFoundError

    @staticmethod
    def _fixup_state_obj(obj, hash_info, fs, path_info, name):
        from ..tree import Tree

        if isinstance(obj, Tree):
            obj.hash_info.nfiles = len(obj)
            for key, entry in obj:
                entry.fs = fs
                entry.path_info = path_info.joinpath(*key)
        else:
            obj.fs = fs
            obj.path_info = path_info
        assert obj.hash_info.name == name
        obj.hash_info.size = hash_info.size

    def stage(
        self,
        scheme: str,
        path_info: "DvcPath",
        fs: "BaseFileSystem",
        name: str,
        **kwargs,
    ) -> "HashFile":
        from ..stage import stage as ostage

        try:
            obj = self.load_from_state(scheme, path_info, fs, name)
            return obj
        except FileNotFoundError:
            pass

        odb = self.get_staging(scheme)
        if odb is None:
            odb = self._odb[scheme]
        return ostage(odb, path_info, fs, name, **kwargs)

    def check(self, scheme: str, obj: "HashFile"):
        from .. import check as ocheck

        return ocheck(
            (
                odb
                for odb in (self._odb.get(scheme), self.get_staging(scheme))
                if odb is not None
            ),
            obj,
        )
