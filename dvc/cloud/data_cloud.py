from dvc.cloud.instance_manager import CloudSettings
from dvc.logger import Logger
from dvc.exceptions import DvcException
from dvc.config import ConfigError
from dvc.progress import progress

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

    def __init__(self, cache_dir, config):
        self._config = config

        cloud_type = self._config['Global'].get('Cloud', '').strip().upper()
        if cloud_type not in self.CLOUD_MAP.keys():
            raise ConfigError('Wrong cloud type %s specified' % cloud_type)

        if cloud_type not in self._config.keys():
            raise ConfigError('Can\'t find cloud section \'[%s]\' in config' % cloud_type)

        cloud_settings = self.get_cloud_settings(cache_dir,
                                                 self._config,
                                                 cloud_type)

        self.typ = cloud_type
        self._cloud = self.CLOUD_MAP[cloud_type](cloud_settings)

        self.sanity_check()

    @staticmethod
    def get_cloud_settings(cache_dir, config, cloud_type):
        """
        Obtain cloud settings from config.
        """
        if cloud_type not in config.keys():
            cloud_config = None
        else:
            cloud_config = config[cloud_type]
        global_storage_path = config['Global'].get('StoragePath', None)
        cloud_settings = CloudSettings(cache_dir, global_storage_path, cloud_config)
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

    def _process_targets(self, func, targets):
        """
        Process targets with a progress bar.
        """
        ret = []
        progress.set_n_total(len(targets))
        try:
            for target in targets:
                ret += [func(target)]
        except Exception as exc:
            raise
        finally:
            progress.finish()

        return list(zip(targets, ret))

    def push(self, targets):
        """
        Push data items in a cloud-agnostic way.
        """
        return self._process_targets(self._cloud.push, targets)

    def pull(self, targets):
        """
        Pull data items in a cloud-agnostic way.
        """
        return self._process_targets(self._cloud.pull, targets)

    def status(self, targets):
        """
        Check status of data items in a cloud-agnostic way.
        """
        return self._process_targets(self._cloud.status, targets)
