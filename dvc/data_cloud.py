"""Manages dvc remotes that user can use with push/pull/status commands."""

import logging
from typing import TYPE_CHECKING, Iterable, Optional, Set, Tuple

from dvc.config import NoRemoteError, RemoteConfigError
from dvc.fs.callbacks import Callback
from dvc.utils.objects import cached_property
from dvc_data.hashfile.db import get_index
from dvc_data.hashfile.transfer import TransferResult

if TYPE_CHECKING:
    from dvc.fs import FileSystem
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.status import CompareStatusResult

logger = logging.getLogger(__name__)


class Remote:
    def __init__(self, name: str, path: str, fs: "FileSystem", *, index=None, **config):
        self.path = path
        self.fs = fs
        self.name = name
        self.index = index

        self.worktree: bool = config.pop("worktree", False)
        self.config = config

    @cached_property
    def odb(self) -> "HashFileDB":
        from dvc.cachemgr import CacheManager
        from dvc_data.hashfile.db import get_odb
        from dvc_data.hashfile.hash import DEFAULT_ALGORITHM

        path = self.path
        if self.worktree:
            path = self.fs.path.join(
                path, ".dvc", CacheManager.FILES_DIR, DEFAULT_ALGORITHM
            )
        else:
            path = self.fs.path.join(path, CacheManager.FILES_DIR, DEFAULT_ALGORITHM)
        return get_odb(self.fs, path, hash_name=DEFAULT_ALGORITHM, **self.config)

    @cached_property
    def legacy_odb(self) -> "HashFileDB":
        from dvc_data.hashfile.db import get_odb

        path = self.path
        return get_odb(self.fs, path, hash_name="md5-dos2unix", **self.config)


def _split_legacy_hash_infos(
    hash_infos: Iterable["HashInfo"],
) -> Tuple[Set["HashInfo"], Set["HashInfo"]]:
    from dvc.cachemgr import LEGACY_HASH_NAMES

    legacy = set()
    default = set()
    for hi in hash_infos:
        if hi.name in LEGACY_HASH_NAMES:
            legacy.add(hi)
        else:
            default.add(hi)
    return legacy, default


class DataCloud:
    """Class that manages dvc remotes.

    Args:
        repo (dvc.repo.Repo): repo instance that belongs to the repo that
            we are working on.

    Raises:
        config.ConfigError: thrown when config has invalid format.
    """

    def __init__(self, repo):
        self.repo = repo

    def get_remote(
        self,
        name: Optional[str] = None,
        command: str = "<command>",
    ) -> "Remote":
        if not name:
            name = self.repo.config["core"].get("remote")

        if name:
            from dvc.fs import get_cloud_fs

            cls, config, fs_path = get_cloud_fs(self.repo.config, name=name)

            if config.get("worktree"):
                version_aware = config.get("version_aware")
                if version_aware is False:
                    raise RemoteConfigError(
                        "worktree remotes require version_aware cloud"
                    )
                if version_aware is None:
                    config["version_aware"] = True

            fs = cls(**config)
            config["tmp_dir"] = self.repo.site_cache_dir
            if self.repo.data_index is not None:
                index = self.repo.data_index.view(("remote", name))
            else:
                index = None
            return Remote(name, fs_path, fs, index=index, **config)

        if bool(self.repo.config["remote"]):
            error_msg = (
                f"no remote specified in {self.repo}. Setup default remote with\n"
                "    dvc remote default <remote name>\n"
                "or use:\n"
                f"    dvc {command} -r <remote name>"
            )
        else:
            error_msg = (
                f"no remote specified in {self.repo}. Create a default remote with\n"
                "    dvc remote add -d <remote name> <remote url>"
            )

        raise NoRemoteError(error_msg)

    def get_remote_odb(
        self,
        name: Optional[str] = None,
        command: str = "<command>",
        hash_name: str = "md5",
    ) -> "HashFileDB":
        from dvc.cachemgr import LEGACY_HASH_NAMES

        remote = self.get_remote(name=name, command=command)
        if remote.fs.version_aware or remote.worktree:
            raise NoRemoteError(
                f"'{command}' is unsupported for cloud versioned remotes"
            )
        if hash_name in LEGACY_HASH_NAMES:
            return remote.legacy_odb
        return remote.odb

    def _log_missing(self, status: "CompareStatusResult"):
        if status.missing:
            missing_desc = "\n".join(
                f"name: {hash_info.obj_name}, {hash_info}"
                for hash_info in status.missing
            )
            logger.warning(
                (
                    "Some of the cache files do not exist neither locally "
                    "nor on remote. Missing cache files:\n%s"
                ),
                missing_desc,
            )

    def transfer(
        self,
        src_odb: "HashFileDB",
        dest_odb: "HashFileDB",
        objs: Iterable["HashInfo"],
        **kwargs,
    ) -> "TransferResult":
        from dvc_data.hashfile.transfer import transfer

        return transfer(src_odb, dest_odb, objs, **kwargs)

    def push(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["HashFileDB"] = None,
    ) -> "TransferResult":
        """Push data items in a cloud-agnostic way.

        Args:
            objs: objects to push to the cloud.
            jobs: number of jobs that can be running simultaneously.
            remote: optional name of remote to push to.
                By default remote from core.remote config option is used.
            odb: optional ODB to push to. Overrides remote.
        """
        if odb is not None:
            return self._push(objs, jobs=jobs, odb=odb)
        legacy_objs, default_objs = _split_legacy_hash_infos(objs)
        result = TransferResult(set(), set())
        if legacy_objs:
            odb = self.get_remote_odb(remote, "push", hash_name="md5-dos2unix")
            t, f = self._push(legacy_objs, jobs=jobs, odb=odb)
            result.transferred.update(t)
            result.failed.update(f)
        if default_objs:
            odb = self.get_remote_odb(remote, "push")
            t, f = self._push(default_objs, jobs=jobs, odb=odb)
            result.transferred.update(t)
            result.failed.update(f)
        return result

    def _push(
        self,
        objs: Iterable["HashInfo"],
        *,
        jobs: Optional[int] = None,
        odb: "HashFileDB",
    ) -> "TransferResult":
        if odb.hash_name == "md5-dos2unix":
            cache = self.repo.cache.legacy
        else:
            cache = self.repo.cache.local
        with Callback.as_tqdm_callback(
            desc=f"Pushing to {odb.fs.unstrip_protocol(odb.path)}",
            unit="file",
        ) as cb:
            return self.transfer(
                cache,
                odb,
                objs,
                jobs=jobs,
                dest_index=get_index(odb),
                cache_odb=cache,
                validate_status=self._log_missing,
                callback=cb,
            )

    def pull(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["HashFileDB"] = None,
    ) -> "TransferResult":
        """Pull data items in a cloud-agnostic way.

        Args:
            objs: objects to pull from the cloud.
            jobs: number of jobs that can be running simultaneously.
            remote: optional name of remote to pull from.
                By default remote from core.remote config option is used.
            odb: optional ODB to pull from. Overrides remote.
        """
        if odb is not None:
            return self._pull(objs, jobs=jobs, odb=odb)
        legacy_objs, default_objs = _split_legacy_hash_infos(objs)
        result = TransferResult(set(), set())
        if legacy_objs:
            odb = self.get_remote_odb(remote, "pull", hash_name="md5-dos2unix")
            assert odb.hash_name == "md5-dos2unix"
            t, f = self._pull(legacy_objs, jobs=jobs, odb=odb)
            result.transferred.update(t)
            result.failed.update(f)
        if default_objs:
            odb = self.get_remote_odb(remote, "pull")
            t, f = self._pull(default_objs, jobs=jobs, odb=odb)
            result.transferred.update(t)
            result.failed.update(f)
        return result

    def _pull(
        self,
        objs: Iterable["HashInfo"],
        *,
        jobs: Optional[int] = None,
        odb: "HashFileDB",
    ) -> "TransferResult":
        if odb.hash_name == "md5-dos2unix":
            cache = self.repo.cache.legacy
        else:
            cache = self.repo.cache.local
        with Callback.as_tqdm_callback(
            desc=f"Fetching from {odb.fs.unstrip_protocol(odb.path)}",
            unit="file",
        ) as cb:
            return self.transfer(
                odb,
                cache,
                objs,
                jobs=jobs,
                src_index=get_index(odb),
                cache_odb=cache,
                verify=odb.verify,
                validate_status=self._log_missing,
                callback=cb,
            )

    def status(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["HashFileDB"] = None,
    ):
        """Check status of data items in a cloud-agnostic way.

        Args:
            objs: objects to check status for.
            jobs: number of jobs that can be running simultaneously.
            remote: optional remote to compare
                cache to. By default remote from core.remote config option
                is used.
            odb: optional ODB to check status from. Overrides remote.
        """
        from dvc_data.hashfile.status import CompareStatusResult

        if odb is not None:
            return self._status(objs, jobs=jobs, odb=odb)
        result = CompareStatusResult(set(), set(), set(), set())
        legacy_objs, default_objs = _split_legacy_hash_infos(objs)
        if legacy_objs:
            odb = self.get_remote_odb(remote, "status", hash_name="md5-dos2unix")
            assert odb.hash_name == "md5-dos2unix"
            o, m, n, d = self._status(legacy_objs, jobs=jobs, odb=odb)
            result.ok.update(o)
            result.missing.update(m)
            result.new.update(n)
            result.deleted.update(d)
        if default_objs:
            odb = self.get_remote_odb(remote, "status")
            o, m, n, d = self._status(default_objs, jobs=jobs, odb=odb)
            result.ok.update(o)
            result.missing.update(m)
            result.new.update(n)
            result.deleted.update(d)
        return result

    def _status(
        self,
        objs: Iterable["HashInfo"],
        *,
        jobs: Optional[int] = None,
        odb: "HashFileDB",
    ):
        from dvc_data.hashfile.status import compare_status

        if odb.hash_name == "md5-dos2unix":
            cache = self.repo.cache.legacy
        else:
            cache = self.repo.cache.local
        return compare_status(
            cache,
            odb,
            objs,
            jobs=jobs,
            dest_index=get_index(odb),
            cache_odb=cache,
        )

    def get_url_for(self, remote, checksum):
        odb = self.get_remote_odb(remote)
        path = odb.oid_to_path(checksum)
        return odb.fs.unstrip_protocol(path)
