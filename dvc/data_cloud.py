"""Manages dvc remotes that user can use with push/pull/status commands."""

import logging
from typing import TYPE_CHECKING, Iterable, Optional

from funcy import cached_property

from dvc_data.hashfile.db import get_index

if TYPE_CHECKING:
    from dvc_data.hashfile.db import HashFileDB
    from dvc_data.hashfile.hash_info import HashInfo
    from dvc_data.hashfile.status import CompareStatusResult
    from dvc_data.hashfile.transfer import TransferResult

logger = logging.getLogger(__name__)


class Remote:
    def __init__(self, path, fs, **config):
        self.path = path
        self.fs = fs

        self.worktree = config.pop("worktree", False)
        self.config = config

    @cached_property
    def odb(self):
        from dvc_data.hashfile.db import get_odb

        path = self.path
        if self.worktree:
            path = self.fs.path.join(path, ".dvc", "cache")

        return get_odb(self.fs, path, **self.config)


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
        from dvc.config import NoRemoteError

        if not name:
            name = self.repo.config["core"].get("remote")

        if name:
            from dvc.fs import get_cloud_fs

            cls, config, fs_path = get_cloud_fs(self.repo, name=name)
            fs = cls(**config)
            config["tmp_dir"] = self.repo.index_db_dir
            return Remote(fs_path, fs, **config)

        if bool(self.repo.config["remote"]):
            error_msg = (
                "no remote specified. Setup default remote with\n"
                "    dvc remote default <remote name>\n"
                "or use:\n"
                "    dvc {} -r <remote name>".format(command)
            )
        else:
            error_msg = (
                "no remote specified. Create a default remote with\n"
                "    dvc remote add -d <remote name> <remote url>"
            )

        raise NoRemoteError(error_msg)

    def get_remote_odb(
        self,
        name: Optional[str] = None,
        command: str = "<command>",
    ) -> "HashFileDB":
        remote = self.get_remote(name=name, command=command)
        return remote.odb

    def _log_missing(self, status: "CompareStatusResult"):
        if status.missing:
            missing_desc = "\n".join(
                f"name: {hash_info.obj_name}, {hash_info}"
                for hash_info in status.missing
            )
            logger.warning(
                "Some of the cache files do not exist neither locally "
                f"nor on remote. Missing cache files:\n{missing_desc}"
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
        odb = odb or self.get_remote_odb(remote, "push")
        return self.transfer(
            self.repo.odb.local,
            odb,
            objs,
            jobs=jobs,
            dest_index=get_index(odb),
            cache_odb=self.repo.odb.local,
            validate_status=self._log_missing,
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
        odb = odb or self.get_remote_odb(remote, "pull")
        return self.transfer(
            odb,
            self.repo.odb.local,
            objs,
            jobs=jobs,
            src_index=get_index(odb),
            cache_odb=self.repo.odb.local,
            verify=odb.verify,
            validate_status=self._log_missing,
        )

    def status(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["HashFileDB"] = None,
        log_missing: bool = True,
    ):
        """Check status of data items in a cloud-agnostic way.

        Args:
            objs: objects to check status for.
            jobs: number of jobs that can be running simultaneously.
            remote: optional remote to compare
                cache to. By default remote from core.remote config option
                is used.
            odb: optional ODB to check status from. Overrides remote.
            log_missing: log warning messages if file doesn't exist
                neither in cache, neither in cloud.
        """
        from dvc_data.hashfile.status import compare_status

        if not odb:
            odb = self.get_remote_odb(remote, "status")
        return compare_status(
            self.repo.odb.local,
            odb,
            objs,
            jobs=jobs,
            log_missing=log_missing,
            dest_index=get_index(odb),
            cache_odb=self.repo.odb.local,
        )

    def get_url_for(self, remote, checksum):
        odb = self.get_remote_odb(remote)
        path = odb.oid_to_path(checksum)
        return odb.fs.unstrip_protocol(path)
