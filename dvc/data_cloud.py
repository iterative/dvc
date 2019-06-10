"""Manages dvc remotes that user can use with push/pull/status comamnds."""

from __future__ import unicode_literals

import logging

from dvc.config import Config, ConfigError
from dvc.remote import Remote
from dvc.remote.s3 import RemoteS3
from dvc.remote.gs import RemoteGS
from dvc.remote.azure import RemoteAZURE
from dvc.remote.oss import RemoteOSS
from dvc.remote.ssh import RemoteSSH
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.local import RemoteLOCAL
from dvc.remote.http import RemoteHTTP


logger = logging.getLogger(__name__)


class DataCloud(object):
    """Class that manages dvc remotes.

    Args:
        repo (dvc.repo.Repo): repo instance that belongs to the repo that
            we are working on.

    Raises:
        config.ConfigError: thrown when config has invalid format.
    """

    CLOUD_MAP = {
        "aws": RemoteS3,
        "gcp": RemoteGS,
        "azure": RemoteAZURE,
        "oss": RemoteOSS,
        "ssh": RemoteSSH,
        "hdfs": RemoteHDFS,
        "local": RemoteLOCAL,
        "http": RemoteHTTP,
        "https": RemoteHTTP,
    }

    def __init__(self, repo):
        self.repo = repo

    @property
    def _config(self):
        return self.repo.config.config

    @property
    def _core(self):
        return self._config.get(Config.SECTION_CORE, {})

    def get_remote(self, remote=None, command="<command>"):
        if not remote:
            remote = self._core.get(Config.SECTION_CORE_REMOTE)

        if remote:
            return self._init_remote(remote)

        # Old config format support for backward compatibility
        if Config.SECTION_CORE_CLOUD in self._core:
            msg = "using obsoleted config format. Consider updating."
            logger.warning(msg)
            return self._init_compat()

        raise ConfigError(
            "No remote repository specified. Setup default repository with\n"
            "    dvc config core.remote <name>\n"
            "or use:\n"
            "    dvc {} -r <name>\n".format(command)
        )

    def _init_remote(self, remote):
        return Remote(self.repo, name=remote)

    def _init_compat(self):
        name = self._core.get(Config.SECTION_CORE_CLOUD, "").strip().lower()
        if name == "":
            return None

        cloud_type = self.CLOUD_MAP.get(name, None)
        if not cloud_type:
            msg = "wrong cloud type '{}' specified".format(name)
            raise ConfigError(msg)

        cloud_config = self._config.get(name, None)
        if not cloud_config:
            msg = "can't find cloud section '{}' in config".format(name)
            raise ConfigError(msg)

        # NOTE: check if the class itself has everything needed for operation.
        # E.g. all the imported packages.
        if not cloud_type.supported(cloud_type.compat_config(cloud_config)):
            raise ConfigError("unsupported cloud '{}'".format(name))

        return self._init_cloud(cloud_config, cloud_type)

    def _init_cloud(self, cloud_config, cloud_type):
        global_storage_path = self._core.get(Config.SECTION_CORE_STORAGEPATH)
        if global_storage_path:
            logger.warning("using obsoleted config format. Consider updating.")

        cloud = cloud_type(self.repo, cloud_config)
        return cloud

    def push(self, targets, jobs=None, remote=None, show_checksums=False):
        """Push data items in a cloud-agnostic way.

        Args:
            targets (list): list of targets to push to the cloud.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.RemoteBASE): optional remote to push to.
                By default remote from core.remote config option is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        return self.repo.cache.local.push(
            targets,
            jobs=jobs,
            remote=self.get_remote(remote, "push"),
            show_checksums=show_checksums,
        )

    def pull(self, targets, jobs=None, remote=None, show_checksums=False):
        """Pull data items in a cloud-agnostic way.

        Args:
            targets (list): list of targets to pull from the cloud.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.RemoteBASE): optional remote to pull from.
                By default remote from core.remote config option is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        return self.repo.cache.local.pull(
            targets,
            jobs=jobs,
            remote=self.get_remote(remote, "pull"),
            show_checksums=show_checksums,
        )

    def status(self, targets, jobs=None, remote=None, show_checksums=False):
        """Check status of data items in a cloud-agnostic way.

        Args:
            targets (list): list of targets to check status for.
            jobs (int): number of jobs that can be running simultaneously.
            remote (dvc.remote.base.RemoteBASE): optional remote to compare
                targets to. By default remote from core.remote config option
                is used.
            show_checksums (bool): show checksums instead of file names in
                information messages.
        """
        remote = self.get_remote(remote, "status")
        return self.repo.cache.local.status(
            targets, jobs=jobs, remote=remote, show_checksums=show_checksums
        )
