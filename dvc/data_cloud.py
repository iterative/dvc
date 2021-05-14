"""Manages dvc remotes that user can use with push/pull/status commands."""

import logging
from typing import TYPE_CHECKING, Iterable, Optional

if TYPE_CHECKING:
    from dvc.objects.file import HashFile
    from dvc.remote.base import Remote

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

    def get_remote(
        self, name: Optional[str] = None, command: str = "<command>",
    ) -> "Remote":
        from dvc.config import NoRemoteError

        if not name:
            name = self.repo.config["core"].get("remote")

        if name:
            return self._init_remote(name)

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

    def _init_remote(self, name):
        from dvc.remote import get_remote

        return get_remote(self.repo, name=name)

    def push(
        self,
        objs: Iterable["HashFile"],
        jobs: Optional[int] = None,
        remote_name: Optional[str] = None,
        show_checksums: bool = False,
    ):
        """Push data items in a cloud-agnostic way.

        Args:
            objs: objects to push to the cloud.
            jobs: number of jobs that can be running simultaneously.
            remote: optional remote to push to.
                By default remote from core.remote config option is used.
            show_checksums: show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote_name, "push")

        return remote.push(
            self.repo.odb.local,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
        )

    def pull(
        self,
        objs: Iterable["HashFile"],
        jobs: Optional[int] = None,
        remote_name: Optional[str] = None,
        show_checksums: bool = False,
    ):
        """Pull data items in a cloud-agnostic way.

        Args:
            objs: objects to pull from the cloud.
            jobs: number of jobs that can be running simultaneously.
            remote: optional remote to pull from.
                By default remote from core.remote config option is used.
            show_checksums: show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote_name, "pull")

        return remote.pull(
            self.repo.odb.local,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
        )

    def status(
        self,
        objs: Iterable["HashFile"],
        jobs: Optional[int] = None,
        remote_name: Optional[str] = None,
        show_checksums: bool = False,
        log_missing: bool = True,
    ):
        """Check status of data items in a cloud-agnostic way.

        Args:
            objs: objects to check status for.
            jobs: number of jobs that can be running simultaneously.
            remote: optional remote to compare
                cache to. By default remote from core.remote config option
                is used.
            show_checksums: show checksums instead of file names in
                information messages.
            log_missing: log warning messages if file doesn't exist
                neither in cache, neither in cloud.
        """
        remote = self.get_remote(remote_name, "status")
        return remote.status(
            self.repo.odb.local,
            objs,
            jobs=jobs,
            show_checksums=show_checksums,
            log_missing=log_missing,
        )

    def get_url_for(self, remote, checksum):
        remote = self.get_remote(remote)
        return str(remote.odb.hash_to_path_info(checksum))
