import os
import configparser

from dvc.exceptions import DvcException
from dvc.logger import Logger
from dvc.utils import cached_property


class ConfigError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class ConfigI(object):
    TARGET_FILE_DEFAULT = '.target'

    def __init__(self, data_dir=None, cache_dir=None, state_dir=None, target_file=None,
                 cloud_config=None):
        self._data_dir = None
        self._cache_dir = None
        self._state_dir = None
        self._target_file = None
        self._cloud_config = None
        self.set(data_dir, cache_dir, state_dir, target_file, cloud_config)

    def set(self, data_dir, cache_dir, state_dir, target_file, cloud_config):
        self._data_dir = data_dir
        self._cache_dir = cache_dir
        self._state_dir = state_dir
        self._target_file = target_file
        self._cloud_config = cloud_config

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
    def target_file(self):
        return self._target_file

    @property
    def cloud_config(self):
        return self._cloud_config


class CloudConfig(object):
    CLOUD_TYPE_AWS = 'AWS'
    CLOUD_TYPE_GCP = 'GCP'
    CLOUD_TYPE_LOCAL = 'LOCAL'

    CLOUD_TYPES = [
        CLOUD_TYPE_AWS,
        CLOUD_TYPE_GCP,
        CLOUD_TYPE_LOCAL
    ]

    def __init__(self, conf_p):
        self._conf_p = conf_p
        self._type = self._conf_p['Global'].get('Cloud', '').strip().upper()

    @cached_property
    def type(self):
        res = self._conf_p['Global'].get('Cloud', '').strip().upper()
        if res not in self.CLOUD_TYPES:
            raise ConfigError(u'Wrong cloud type {} specified'.format(res))

        if res not in self._conf_p.keys():
            raise ConfigError(u'Cannot find cloud section [{}] in config'.format(res))

        return res

    def get(self, name, default=None):
        return self._conf_p.get(self.type).get(name, default)


class Config(ConfigI):
    CONFIG = 'dvc.conf'

    def __init__(self, conf_file=CONFIG, conf_pseudo_file=None):
        """
        Params:
            conf_file (String): configuration file
            conf_pseudo_file (String): for unit testing, something that supports readline; supersedes conf_file
        """
        self._conf_file = conf_file
        self._config = configparser.SafeConfigParser()

        if conf_pseudo_file is not None:
            self._config.readfp(conf_pseudo_file)
        else:
            if not os.path.isfile(conf_file):
                raise ConfigError('Config file "{}" does not exist {}'.format(conf_file, os.getcwd()))
            self._config.read(conf_file)

        level = self._config['Global']['LogLevel']
        Logger.set_level(level)

        cloud_config = CloudConfig(self._config)

        super(Config, self).__init__(self._config['Global']['DataDir'],
                                     self._config['Global']['CacheDir'],
                                     self._config['Global']['StateDir'],
                                     self._config['Global'].get('TargetFile', Config.TARGET_FILE_DEFAULT),
                                     cloud_config)
        pass

    @property
    def file(self):
        return self._conf_file
