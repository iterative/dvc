from multiprocessing.pool import ThreadPool

from dvc.cloud.instance_manager import CloudSettings
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.utils import map_progress
from dvc.config import Config

from dvc.cloud.aws import DataCloudAWS
from dvc.cloud.gcp import DataCloudGCP
from dvc.cloud.local import DataCloudLOCAL
from dvc.cloud.base import DataCloudBase


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'aws'   : DataCloudAWS,
        'gcp'   : DataCloudGCP,
        'local' : DataCloudLOCAL,
    }

    SCHEME_MAP = {
        's3'    : 'aws',
        'gs'    : 'gcp',
        ''      : 'local',
    }

    def __init__(self, cache, config):
        self._config = config

        cloud_type = self._config[Config.SECTION_CORE].get(Config.SECTION_CORE_CLOUD, '').strip().lower()

        if cloud_type == '':
            self.typ = None
            self._cloud = None
            return

        if cloud_type not in self.CLOUD_MAP.keys():
            raise ConfigError('Wrong cloud type %s specified' % cloud_type)

        if cloud_type not in self._config.keys():
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % cloud_type)

        cloud_settings = self.get_cloud_settings(cache,
                                                 self._config,
                                                 cloud_type)

        self.typ = cloud_type
        self._cloud = self.CLOUD_MAP[cloud_type](cloud_settings)
        self._cloud.sanity_check()

    @staticmethod
    def get_cloud_settings(cache, config, cloud_type):
        """
        Obtain cloud settings from config.
        """
        if cloud_type not in config.keys():
            cloud_config = None
        else:
            cloud_config = config[cloud_type]
        global_storage_path = config[Config.SECTION_CORE].get(Config.SECTION_CORE_STORAGEPATH, None)
        cloud_settings = CloudSettings(cache, global_storage_path, cloud_config)
        return cloud_settings

    def _collect(self, targets, jobs, local):
        collected = set()
        pool = ThreadPool(processes=jobs)
        args = zip(targets, [local]*len(targets))
        ret = pool.map(self._cloud.collect, args)

        for r in ret:
            collected |= set(r)

        return collected

    def _map_targets(self, func, targets, jobs, collect_local=False, collect_cloud=False):
        """
        Process targets as data items in parallel.
        """
        if not self._cloud:
            return

        self._cloud.connect()

        collected = set()
        if collect_local:
            collected |= self._collect(targets, jobs, True)
        if collect_cloud:
            collected |= self._collect(targets, jobs, False)

        return map_progress(func, list(collected), jobs)

    def push(self, targets, jobs=1):
        """
        Push data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.push, targets, jobs, collect_local=True)

    def pull(self, targets, jobs=1):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.pull, targets, jobs, collect_cloud=True)

    def status(self, targets, jobs=1):
        """
        Check status of data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.status, targets, jobs, True, True)
