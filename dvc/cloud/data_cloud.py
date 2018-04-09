import re
from multiprocessing.pool import ThreadPool

from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import Config, ConfigError
from dvc.utils import map_progress

from dvc.cloud.aws import DataCloudAWS
from dvc.cloud.gcp import DataCloudGCP
from dvc.cloud.ssh import DataCloudSSH
from dvc.cloud.local import DataCloudLOCAL
from dvc.cloud.base import DataCloudBase, CloudSettings


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'aws'   : DataCloudAWS,
        'gcp'   : DataCloudGCP,
        'ssh'   : DataCloudSSH,
        'local' : DataCloudLOCAL,
    }

    def __init__(self, cache=None, config=None, state=None):
        self._cache = cache
        self._config = config
        self._state = state

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
    def supported(url):
        for cloud in DataCloud.CLOUD_MAP.values():
            if cloud.supported(url):
                return cloud
        return None

    def _init_remote(self, remote):
        section = Config.SECTION_REMOTE_FMT.format(remote)
        cloud_config = self._config.get(section, None)
        if not cloud_config:
            raise ConfigError("Can't find remote section '{}' in config".format(section))

        url = cloud_config[Config.SECTION_REMOTE_URL]
        cloud_type = self.supported(url)
        if not cloud_type:
            raise ConfigError("Unsupported url '{}'".format(url))

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

        cloud_settings = CloudSettings(cache=self._cache,
                                       state=self._state,
                                       global_storage_path=global_storage_path,
                                       cloud_config=cloud_config)

        cloud = cloud_type(cloud_settings)
        cloud.sanity_check()
        return cloud

    def _collect(self, cloud, targets, jobs, local):
        collected = set()
        pool = ThreadPool(processes=jobs)
        args = zip(targets, [local]*len(targets))
        ret = pool.map(cloud.collect, args)

        for r in ret:
            collected |= set(r)

        return collected

    def _map_targets(self, func, targets, jobs, collect_local=False, collect_cloud=False, remote=None):
        """
        Process targets as data items in parallel.
        """

        if not remote:
            cloud = self._cloud
        else:
            cloud = self._init_remote(remote)

        if not cloud:
            return

        cloud.connect()

        collected = set()
        if collect_local:
            collected |= self._collect(cloud, targets, jobs, True)
        if collect_cloud:
            collected |= self._collect(cloud, targets, jobs, False)

        ret = map_progress(getattr(cloud, func), list(collected), jobs)

        cloud.disconnect()

        return ret

    def push(self, targets, jobs=1, remote=None):
        """
        Push data items in a cloud-agnostic way.
        """
        return self._map_targets('push', targets, jobs, collect_local=True, remote=remote)

    def pull(self, targets, jobs=1, remote=None):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self._map_targets('pull', targets, jobs, collect_cloud=True, remote=remote)

    def status(self, targets, jobs=1, remote=None):
        """
        Check status of data items in a cloud-agnostic way.
        """
        return self._map_targets('status', targets, jobs, True, True, remote=remote)
