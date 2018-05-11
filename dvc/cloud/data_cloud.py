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
from dvc.cloud.base import STATUS_MODIFIED, STATUS_NEW, STATUS_DELETED


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'aws'   : DataCloudAWS,
        'gcp'   : DataCloudGCP,
        'ssh'   : DataCloudSSH,
        'local' : DataCloudLOCAL,
    }

    def __init__(self, cache=None, config=None):
        self._cache = cache
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

    def _get_cloud(self, remote):
        if remote:
            return self._init_remote(remote)

        return self._cloud

    def _filter(self, func, status, targets, jobs, remote):
        cloud = self._get_cloud(remote)
        if not cloud:
            return []

        with cloud:
            filtered = []
            for t, s in self._status(cloud, targets, jobs):
                if s == STATUS_MODIFIED or s == status:
                    filtered.append(t)

            return map_progress(getattr(cloud, func), filtered, jobs)

    def push(self, targets, jobs=1, remote=None):
        """
        Push data items in a cloud-agnostic way.
        """
        return self._filter('push', STATUS_NEW, targets, jobs, remote)

    def pull(self, targets, jobs=1, remote=None):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self._filter('pull', STATUS_DELETED, targets, jobs, remote)

    def _collect_targets(self, cloud, targets, jobs=1):
        collected = set()
        collected |= self._collect(cloud, targets, jobs, True)
        collected |= self._collect(cloud, targets, jobs, False)
        return list(collected)

    def _status(self, cloud, targets, jobs=1):
        collected = self._collect_targets(cloud, targets, jobs)
        return map_progress(cloud.status, collected, jobs)

    def status(self, targets, jobs=1, remote=None):
        """
        Check status of data items in a cloud-agnostic way.
        """
        cloud = self._get_cloud(remote)
        if not cloud:
            return []

        with cloud:
            return self._status(cloud, targets, jobs)
