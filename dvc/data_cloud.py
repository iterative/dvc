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

        return self.repo.cache.local.push(
            cache, jobs=jobs, remote=remote, show_checksums=show_checksums,
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

        downloaded_items_num = self.repo.cache.local.pull(
            cache, jobs=jobs, remote=remote, show_checksums=show_checksums
        )

        if not remote.tree.verify:
            self._save_pulled_checksums(cache)

        return downloaded_items_num

    def _save_pulled_checksums(self, cache):
        for checksum in cache.scheme_keys("local"):
            cache_file = self.repo.cache.local.tree.hash_to_path_info(checksum)
            if self.repo.cache.local.tree.exists(cache_file):
                # We can safely save here, as existing corrupted files will
                # be removed upon status, while files corrupted during
                # download will not be moved from tmp_file
                # (see `RemoteBASE.download()`)
                self.repo.state.save(cache_file, checksum)

    def status(self, cache, jobs=None, remote=None, show_checksums=False):
        """Check status of data items in a cloud-agnostic way.

        Args:
            cache (NamedCache): named checksums to check status for.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.BaseRemote): optional remote to compare
                cache to. By default remote from core.remote config option
                is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote, "status")
        return self.repo.cache.local.status(
            cache, jobs=jobs, remote=remote, show_checksums=show_checksums
        )
