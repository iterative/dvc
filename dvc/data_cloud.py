"""Manages dvc remotes that user can use with push/pull/status commands."""

import logging

from dvc.config import NoRemoteError
from dvc.remote import Remote


logger = logging.getLogger(__name__)


class DataCloud(object):
    """Class that manages dvc remotes.

    Args:
        repo (dvc.repo.Repo): repo instance that belongs to the repo that
            we are working on.

    Raises:
        config.ConfigError: thrown when config has invalid format.
    """

    def __init__(self, repo):
        self.repo = repo

    def get_remote(self, remote=None, command="<command>"):
        if not remote:
            remote = self.repo.config["core"].get("remote")

        if remote:
            return self._init_remote(remote)

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

    def _init_remote(self, remote):
        return Remote(self.repo, name=remote)

    def push(
        self,
        caches,
        jobs=None,
        remote=None,
        show_checksums=False,
        drop_index=False,
    ):
        """Push data items in a cloud-agnostic way.

        Args:
            caches (list): list of (dir_cache, file_cache) tuples containing
                named checksums to push to the cloud.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.RemoteBASE): optional remote to push to.
                By default remote from core.remote config option is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        return self.repo.cache.local.push(
            caches,
            jobs=jobs,
            remote=self.get_remote(remote, "push"),
            show_checksums=show_checksums,
            drop_index=drop_index,
        )

    def pull(
        self,
        caches,
        jobs=None,
        remote=None,
        show_checksums=False,
        drop_index=False,
    ):
        """Pull data items in a cloud-agnostic way.

        Args:
            caches (list): list of (dir_cache, file_cache) tuples containing
                named checksums to pull from the cloud.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.RemoteBASE): optional remote to pull from.
                By default remote from core.remote config option is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote, "pull")
        downloaded_items_num = self.repo.cache.local.pull(
            caches,
            jobs=jobs,
            remote=remote,
            show_checksums=show_checksums,
            drop_index=drop_index,
        )

        if not remote.verify:
            self._save_pulled_checksums(caches)

        return downloaded_items_num

    def _save_pulled_checksums(self, cache):
        for dir_cache, file_cache in cache:
            checksums = set(file_cache["local"].keys())
            if dir_cache is not None:
                checksums.update(dir_cache["local"].keys())
            for checksum in checksums:
                cache_file = self.repo.cache.local.checksum_to_path_info(
                    checksum
                )
                if self.repo.cache.local.exists(cache_file):
                    # We can safely save here, as existing corrupted files will
                    # be removed upon status, while files corrupted during
                    # download will not be moved from tmp_file
                    # (see `RemoteBASE.download()`)
                    self.repo.state.save(cache_file, checksum)

    def status(
        self,
        caches,
        jobs=None,
        remote=None,
        show_checksums=False,
        drop_index=False,
    ):
        """Check status of data items in a cloud-agnostic way.

        Args:
            caches (list): list of (dir_cache, file_cache) tuples containg
                named checksums to check status for.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.RemoteBASE): optional remote to compare
                cache to. By default remote from core.remote config option
                is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote, "status")
        return self.repo.cache.local.status(
            caches,
            jobs=jobs,
            remote=remote,
            show_checksums=show_checksums,
            drop_index=drop_index,
        )
