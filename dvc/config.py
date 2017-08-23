import os
import configparser

from dvc.exceptions import DvcException
from dvc.logger import Logger


class ConfigError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class ConfigI(object):
    TARGET_FILE_DEFAULT = '.target'

    def __init__(self, data_dir=None, cache_dir=None, state_dir=None, target_file=None,
                 cloud=None, conf_parser=None):
        self._data_dir = None
        self._cache_dir = None
        self._state_dir = None
        self._target_file = None
        self._conf_parser = None
        self._cloud = None
        self.set(data_dir, cache_dir, state_dir, target_file, cloud, conf_parser)

    def set(self, data_dir, cache_dir, state_dir, target_file, cloud=None, conf_parser=None):
        self._data_dir = data_dir
        self._cache_dir = cache_dir
        self._state_dir = state_dir
        self._target_file = target_file
        self._cloud = cloud
        self._conf_parser = conf_parser

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
    def cloud(self):
        return self._cloud

    @property
    def conf_parser(self):
        return self._conf_parser


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

        super(Config, self).__init__(self._config['Global']['DataDir'],
                                     self._config['Global']['CacheDir'],
                                     self._config['Global']['StateDir'],
                                     self._config['Global'].get('TargetFile', Config.TARGET_FILE_DEFAULT),
                                     self._config['Global']['Cloud'],
                                     self._config)
        pass

    @property
    def file(self):
        return self._conf_file
