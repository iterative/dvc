import re
from multiprocessing.pool import ThreadPool

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import Config, ConfigError

from dvc.remote.s3 import RemoteS3
from dvc.remote.gs import RemoteGS
from dvc.remote.ssh import RemoteSSH
from dvc.remote.hdfs import RemoteHDFS
from dvc.remote.local import RemoteLOCAL
from dvc.remote.base import RemoteBase


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'aws'   : RemoteS3,
        'gcp'   : RemoteGS,
        'ssh'   : RemoteSSH,
        'hdfs'  : RemoteHDFS,
        'local' : RemoteLOCAL,
    }

    def __init__(self, project, config=None):
        self.project = project
        self._config = config

        remote = self._config[Config.SECTION_CORE].get(Config.SECTION_CORE_REMOTE, '')
        if remote == '':
            if config[Config.SECTION_CORE].get(Config.SECTION_CORE_CLOUD, None):
                # backward compatibility
                Logger.warn('Using obsoleted config format. Consider updating.')
                self._cloud = self.__init__compat()
            else:
                self._cloud = None
            return

        self._cloud = self._init_remote(remote)

    @staticmethod
    def supported(config):
        for cloud in DataCloud.CLOUD_MAP.values():
            if cloud.supported(config):
                return cloud
        return None

    def _init_remote(self, remote):
        section = Config.SECTION_REMOTE_FMT.format(remote)
        cloud_config = self._config.get(section, None)
        if not cloud_config:
            raise ConfigError("Can't find remote section '{}' in config".format(section))

        cloud_type = self.supported(cloud_config)
        if not cloud_type:
            raise ConfigError("Unsupported cloud '{}'".format(cloud_config))

        return self._init_cloud(cloud_config, cloud_type)

    def __init__compat(self):
        cloud_name = self._config[Config.SECTION_CORE].get(Config.SECTION_CORE_CLOUD, '').strip().lower()
        if cloud_name == '':
            self._cloud = None
            return

        cloud_type = self.CLOUD_MAP.get(cloud_name, None)
        if not cloud_type:
            raise ConfigError('Wrong cloud type %s specified' % cloud_name)

        cloud_config = self._config.get(cloud_name, None)
        if not cloud_config:
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % cloud_name)

        return self._init_cloud(cloud_config, cloud_type)

    def _init_cloud(self, cloud_config, cloud_type):
        global_storage_path = self._config[Config.SECTION_CORE].get(Config.SECTION_CORE_STORAGEPATH, None)
        if global_storage_path:
            Logger.warn('Using obsoleted config format. Consider updating.')

        cloud = cloud_type(self.project, cloud_config)
        return cloud

    def _get_cloud(self, remote):
        if remote:
            return self._init_remote(remote)

        if self._cloud:
            return self._cloud

        raise ConfigError("No remote repository specified. Setup default repository " \
                          "with 'dvc config core.remote <name>' or use '-r <name>'.")

    def push(self, targets, jobs=1, remote=None):
        """
        Push data items in a cloud-agnostic way.
        """
        return self.project.cache.local.push(targets, jobs=jobs, remote=self._get_cloud(remote))

    def pull(self, targets, jobs=1, remote=None):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self.project.cache.local.pull(targets, jobs=jobs, remote=self._get_cloud(remote))

    def status(self, targets, jobs=1, remote=None):
        """
        Check status of data items in a cloud-agnostic way.
        """
        return self.project.cache.local.status(targets, jobs=jobs, remote=self._get_cloud(remote))
