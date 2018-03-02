from dvc.cloud.instance_manager import CloudSettings
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.utils import map_progress

from dvc.cloud.aws import DataCloudAWS
from dvc.cloud.gcp import DataCloudGCP
from dvc.cloud.local import DataCloudLOCAL


class DataCloud(object):
    """ Generic class to do initial config parsing and redirect to proper DataCloud methods """

    CLOUD_MAP = {
        'AWS'   : DataCloudAWS,
        'GCP'   : DataCloudGCP,
        'LOCAL' : DataCloudLOCAL,
    }

    SCHEME_MAP = {
        's3'    : 'AWS',
        'gs'    : 'GCP',
        ''      : 'LOCAL',
    }

    def __init__(self, cache, config):
        self._config = config

        cloud_type = self._config['Global'].get('Cloud', '').strip().upper()
        if cloud_type not in self.CLOUD_MAP.keys():
            raise ConfigError('Wrong cloud type %s specified' % cloud_type)

        if cloud_type not in self._config.keys():
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % cloud_type)

        cloud_settings = self.get_cloud_settings(cache,
                                                 self._config,
                                                 cloud_type)

        self.typ = cloud_type
        self._cloud = self.CLOUD_MAP[cloud_type](cloud_settings)

        self.sanity_check()

    @staticmethod
    def get_cloud_settings(cache, config, cloud_type):
        """
        Obtain cloud settings from config.
        """
        if cloud_type not in config.keys():
            cloud_config = None
        else:
            cloud_config = config[cloud_type]
        global_storage_path = config['Global'].get('StoragePath', None)
        cloud_settings = CloudSettings(cache, global_storage_path, cloud_config)
        return cloud_settings

    def sanity_check(self):
        """ sanity check a config

        check that we have a cloud and storagePath
        if aws, check can read credentials
        if google, check ProjectName

        Returns:
            (T,) if good
            (F, issues) if bad
        """
        key = 'Cloud'
        if key.lower() not in [k.lower() for k in self._config['Global'].keys()] or len(self._config['Global'][key]) < 1:
            raise ConfigError('Please set %s in section Global in config file' % key)

        # now that a cloud is chosen, can check StoragePath
        storage_path = self._cloud.storage_path
        if storage_path is None or len(storage_path) == 0:
            raise ConfigError('Please set StoragePath = bucket/{optional path} '
                              'in config file in a cloud specific section')

        self._cloud.sanity_check()

    def _map_targets(self, func, targets, jobs):
        """
        Process targets as data items in parallel.
        """
        self._cloud.connect()
        return map_progress(func, targets, jobs)

    def push(self, targets, jobs=1):
        """
        Push data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.push, targets, jobs)

    def pull(self, targets, jobs=1):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.pull, targets, jobs)

    def status(self, targets, jobs=1):
        """
        Check status of data items in a cloud-agnostic way.
        """
        return self._map_targets(self._cloud.status, targets, jobs)
