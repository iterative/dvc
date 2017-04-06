import os
import configparser

from dvc.exceptions import DvcException


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
    def aws_storage_prefix(self):
        return ''


class Config(ConfigI):
    CONFIG = 'dvc.conf'

    def __init__(self, conf_file):
        self._conf_file = conf_file
        self._config = configparser.ConfigParser()

        if not os.path.isfile(conf_file):
            raise ConfigError('Config file "{}" does not exist'.format(conf_file))
        self._config.read(conf_file)

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
    def aws_storage_path(self):
        return self._config['AWS']['StoragePath']

    def _aws_storage_path_parts(self):
        return self.aws_storage_path.strip('/').split('/', 1)

    @property
    def aws_storage_bucket(self):
        return self._aws_storage_path_parts()[0]

    @property
    def aws_storage_prefix(self):
        parts = self._aws_storage_path_parts()
        if len(parts) > 1:
            return parts[1]
        return ''
