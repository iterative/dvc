"""Manages dvc remotes that user can use with push/pull/status commands."""

import logging

from dvc.config import NoRemoteError
from dvc.remote import get_remote

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

    def get_remote(self, name=None, command="<command>"):
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
        return get_remote(self.repo, name=name)

    def push(
        self, cache, jobs=None, remote=None, show_checksums=False,
    ):
        """Push data items in a cloud-agnostic way.

        Args:
            cache (NamedCache): named checksums to push to the cloud.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.BaseRemote): optional remote to push to.
                By default remote from core.remote config option is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote, "push")

        return remote.push(
            self.repo.cache.local,
            cache,
            jobs=jobs,
            show_checksums=show_checksums,
        )

    def pull(
        self, cache, jobs=None, remote=None, show_checksums=False,
    ):
        """Pull data items in a cloud-agnostic way.

        Args:
            cache (NamedCache): named checksums to pull from the cloud.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.BaseRemote): optional remote to pull from.
                By default remote from core.remote config option is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote, "pull")

        return remote.pull(
            self.repo.cache.local,
            cache,
            jobs=jobs,
            show_checksums=show_checksums,
        )

    def status(
        self,
        cache,
        jobs=None,
        remote=None,
        show_checksums=False,
        log_missing=True,
    ):
        """Check status of data items in a cloud-agnostic way.

        Args:
            cache (NamedCache): named checksums to check status for.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.BaseRemote): optional remote to compare
                cache to. By default remote from core.remote config option
                is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
            log_missing (bool): log warning messages if file doesn't exist
                neither in cache, neither in cloud.
        """
        remote = self.get_remote(remote, "status")
        return remote.status(
            self.repo.cache.local,
            cache,
            jobs=jobs,
            show_checksums=show_checksums,
            log_missing=log_missing,
        )

    def get_url_for(self, remote, checksum):
        remote = self.get_remote(remote)
        return str(remote.tree.hash_to_path_info(checksum))
