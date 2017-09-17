import os
import configparser

from dvc.exceptions import DvcException
from dvc.logger import Logger


class ConfigError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Config file error: {}'.format(msg))


class ConfigI(object):
    CONFIG_DIR          = '.dvc'
    TARGET_FILE_DEFAULT = 'target'
    CONFIG              = 'config'
    STATE_DIR           = 'state'
    CACHE_DIR           = 'cache'

    def __init__(self, data_dir=None, cloud=None, conf_parser=None):
        self._data_dir = None
        self._conf_parser = None
        self._cloud = None
        self.set(data_dir, cloud, conf_parser)

    def set(self, data_dir, cloud=None, conf_parser=None):
        self._data_dir = data_dir
        self._cloud = cloud
        self._conf_parser = conf_parser

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def cache_dir(self):
        return os.path.join(self.CONFIG_DIR, self.CACHE_DIR)

    @property
    def state_dir(self):
        return os.path.join(self.CONFIG_DIR, self.STATE_DIR)

    @property
    def cloud(self):
        return self._cloud

    @property
    def conf_parser(self):
        return self._conf_parser


class Config(ConfigI):
    def __init__(self, conf_file=ConfigI.CONFIG, conf_pseudo_file=None):
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
            fname = os.path.join(self.CONFIG_DIR, conf_file)
            if not os.path.isfile(fname):
                raise ConfigError('Config file "{}" does not exist {}'.format(fname, os.getcwd()))
            self._config.read(fname)

        level = self._config['Global']['LogLevel']
        Logger.set_level(level)

        super(Config, self).__init__(self._config['Global']['DataDir'],
                                     self._config['Global']['Cloud'],
                                     self._config)
        pass

    @property
    def file(self):
        return self._conf_file
