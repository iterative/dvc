"""Manages dvc remotes that user can use with push/pull/status commands."""

import logging
from typing import TYPE_CHECKING, Iterable, Optional

from dvc.data.db import get_index

if TYPE_CHECKING:
    from dvc.hash_info import HashInfo
    from dvc.objects.db import ObjectDB

logger = logging.getLogger(__name__)


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

    def get_remote_odb(
        self,
        name: Optional[str] = None,
        command: str = "<command>",
    ) -> "ObjectDB":
        from dvc.config import NoRemoteError

        if not name:
            name = self.repo.config["core"].get("remote")

        if name:
            return self._init_odb(name)

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

    def _init_odb(self, name):
        from dvc.data.db import get_odb
        from dvc.fs import get_cloud_fs

        cls, config, fs_path = get_cloud_fs(self.repo, name=name)
        config["tmp_dir"] = self.repo.index_db_dir
        return get_odb(cls(**config), fs_path, **config)

    def push(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["ObjectDB"] = None,
    ):
        """Push data items in a cloud-agnostic way.

        Args:
            objs: objects to push to the cloud.
            jobs: number of jobs that can be running simultaneously.
            remote: optional name of remote to push to.
                By default remote from core.remote config option is used.
            odb: optional ODB to push to. Overrides remote.
        """
        from dvc.data.transfer import transfer

        if not odb:
            odb = self.get_remote_odb(remote, "push")
        return transfer(
            self.repo.odb.local,
            odb,
            objs,
            jobs=jobs,
            dest_index=get_index(odb),
            cache_odb=self.repo.odb.local,
        )

    def pull(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["ObjectDB"] = None,
    ):
        """Pull data items in a cloud-agnostic way.

        Args:
            objs: objects to pull from the cloud.
            jobs: number of jobs that can be running simultaneously.
            remote: optional name of remote to pull from.
                By default remote from core.remote config option is used.
            odb: optional ODB to pull from. Overrides remote.
        """
        from dvc.data.transfer import transfer

        if not odb:
            odb = self.get_remote_odb(remote, "pull")
        return transfer(
            odb,
            self.repo.odb.local,
            objs,
            jobs=jobs,
            src_index=get_index(odb),
            cache_odb=self.repo.odb.local,
            verify=odb.verify,
        )

    def status(
        self,
        objs: Iterable["HashInfo"],
        jobs: Optional[int] = None,
        remote: Optional[str] = None,
        odb: Optional["ObjectDB"] = None,
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
        from dvc.data.status import compare_status

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
        path = odb.hash_to_path(checksum)
        return odb.fs.unstrip_protocol(path)
