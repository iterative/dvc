import os
import configparser

from dvc.exceptions import DvcException
from dvc.logger import Logger


class ConfigError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class ConfigI(object):
    def __init__(self, data_dir=None, cache_dir=None, state_dir=None):
        self._data_dir = data_dir
        self._cache_dir = cache_dir
        self._state_dir = state_dir
        pass

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def cache_dir(self):
        return self._cache_dir

    @property
    def state_dir(self):
        return self._state_dir

    @property
    def storage_prefix(self):
        return ''


class Config(ConfigI):
    CONFIG = 'dvc.conf'

    def __init__(self, conf_file):
        self._conf_file = conf_file
        self._config = configparser.ConfigParser()

        if not os.path.isfile(conf_file):
            raise ConfigError('Config file "{}" does not exist'.format(conf_file))
        self._config.read(conf_file)

        level = self._config['Global']['LogLevel']
        Logger.set_level(level)

        super(Config, self).__init__(self._config['Global']['DataDir'],
                                     self._config['Global']['CacheDir'],
                                     self._config['Global']['StateDir'])
        pass

    @property
    def file(self):
        return self._conf_file

    @property
    def aws_access_key_id(self):
        return self._config['AWS']['AccessKeyId']

    @property
    def aws_secret_access_key(self):
        return self._config['AWS']['SecretAccessKey']

    @property
    def storage_path(self):
        """ get storage path

        Precedence: Storage, then cloud specific
        """

        path = self._config['Data'].get('StoragePath', None)
        if path:
            return path

        cloud = self.get_cloud
        assert cloud in ['amazon', 'google'], 'unknown cloud %s' % cloud
        if cloud == 'amazon':
            path = self._config['AWS'].get('StoragePath', None)
        elif cloud == 'google':
            path = self._config['GC'].get('StoragePath', None)
        if path is None:
            raise ConfigError('invalid StoragePath: not set for Data or cloud specific')
        return path

    def _storage_path_parts(self):
        return self.storage_path.strip('/').split('/', 1)

    @property
    def storage_bucket(self):
        """ Data -> StoragePath takes precedence; if doesn't exist, use cloud-specific """
        return self._storage_path_parts()[0]

    @property
    def storage_prefix(self):
        parts = self._storage_path_parts()
        if len(parts) > 1:
            return parts[1]
        return ''

    @property
    def gc_project_name(self):
        return self._config['GC']['ProjectName']

    @property
    def get_cloud(self):
        """ get cloud choice: currently one of ['amazon', 'google'] """
        return self._config['Global']['Cloud']
