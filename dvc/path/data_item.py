import os

from dvc.path.path import DvcPath
from dvc.exceptions import DvcException
from dvc.utils import cached_property


class DataFilePathError(DvcException):
    def __init__(self, msg):
        DvcException.__init__(self, 'Data file error: {}'.format(msg))


class NotInDataDirError(DvcException):
    def __init__(self, file, data_dir):
        DvcException.__init__(self,
                                   'Data file location error: the file "{}" has to be in the data directory "{}"'.
                              format(file, data_dir))


class DataPath(object):
    STATE_FILE_SUFFIX = '.state'
    CACHE_FILE_SEP = '_'

    def __init__(self, data_file, git, config, cache_file=None):
        self._git = git
        self._config = config
        self._cache_file = cache_file

        self._data = DvcPath(data_file, git)
        if not self._data.abs.startswith(self.data_dir_abs):
            raise NotInDataDirError(data_file, self._config.data_dir)

        if not self._data.dvc.startswith(self._config.data_dir):
            raise NotInDataDirError(data_file, self._config.data_dir)
        pass

    @property
    def data(self):
        return self._data

    @property
    def data_dvc_short(self):
        return self._data.dvc[len(self._config.data_dir)+1:]

    @cached_property
    def state(self):
        state_dir = os.path.join(self._git.git_dir_abs, self._config.state_dir)
        state_file = os.path.join(state_dir, self.data_dvc_short + self.STATE_FILE_SUFFIX)
        return DvcPath(state_file, self._git)

    @cached_property
    def cache(self):
        if self._cache_file:
            return DvcPath(self._cache_file, self._git)
        else:
            cache_dir = os.path.join(self._git.git_dir_abs, self._config.cache_dir)
            cache_file_suffix = self.CACHE_FILE_SEP + self._git.curr_commit
            cache_file = os.path.join(cache_dir, self.data_dvc_short + cache_file_suffix)
            return DvcPath(cache_file, self._git)

    @cached_property
    def data_dir_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.data_dir)

    @property
    def _symlink_file(self):
        data_file_dir = os.path.dirname(self.data.relative)
        return os.path.relpath(self.cache.relative, data_file_dir)

    def create_symlink(self):
        os.symlink(self._symlink_file, self.data.relative)
