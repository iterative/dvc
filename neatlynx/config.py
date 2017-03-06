import os
import configparser

from neatlynx.exceptions import NeatLynxException


class ConfigError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Config file error: {}'.format(msg))


class ConfigI(object):
    def __init__(self, data_dir, cache_dir, state_dir):
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


class Config(ConfigI):
    CONFIG = 'neatlynx.conf'

    def __init__(self, git_dir, file=None, data_dir=None, cache_dir=None, state_dir=None):
        self._config = configparser.ConfigParser()
        self._config.read(os.path.join(git_dir, self.CONFIG))

        self._global = self._config['Global']

        self._data_dir = self._global['DataDir']
        self._cache_dir = self._global['CacheDir']
        self._state_dir = self._global['StateDir']
        pass

    # def get_data_file_obj(self):
    #     return DataFileObj(self.config)

    @property
    def data_dir(self):
        return self._data_dir

    @property
    def cache_dir(self):
        return self._cache_dir

    @property
    def state_dir(self):
        return self._state_dir