import os
import stat

from dvc.config import ConfigI
from dvc.path.path import Path
from dvc.exceptions import DvcException
from dvc.system import System
from dvc.utils import cached_property
from dvc.data_cloud import file_md5
from dvc.state_file import StateFile, LocalStateFile


class DataItemError(DvcException):
    def __init__(self, msg):
        super(DataItemError, self).__init__('Data item error: {}'.format(msg))


class DataDirError(DvcException):
    def __init__(self, msg):
        super(DataDirError, self).__init__(msg)


class DataItemInConfigDirError(DataDirError):
    def __init__(self, file):
        msg = 'File "{}" is in config directory'.format(file)
        super(DataDirError, self).__init__(msg)


class NotInGitDirError(DataDirError):
    def __init__(self, file, git_dir):
        msg = 'File "{}" is not in git directory "{}"'.format(file, git_dir)
        super(NotInGitDirError, self).__init__(msg)


class DataItem(object):
    STATE_FILE_SUFFIX = '.dvc'
    LOCAL_STATE_FILE_PREFIX = '.'
    LOCAL_STATE_FILE_SUFFIX = '.dvc_local_state'
    CACHE_FILE_SEP = '_'

    def __init__(self, data_file, git, config, cache_file=None):
        self._git = git
        self._config = config
        self._cache_file = cache_file

        self._data = Path(data_file, git)

        if not self._data.abs.startswith(self._git.git_dir_abs):
            raise NotInGitDirError(data_file, self._git.git_dir_abs)

        if self._data.abs.startswith(os.path.join(self._git.git_dir_abs, self._config.CONFIG_DIR)):
            raise DataItemInConfigDirError(data_file)

    def copy(self, cache_file=None):
        if not cache_file:
            cache_file = self._cache_file

        return DataItem(self._data.abs, self._git, self._config, cache_file)

    def __hash__(self):
        return self.data.dvc.__hash__()

    def __eq__(self, other):
        if other == None:
            return False

        return self.data.dvc == other.data.dvc

    @property
    def data(self):
        return self._data

    def _state(self, prefix, suffix):
        state_file = os.path.join(self._git.git_dir_abs,
                                  os.path.dirname(self.data.dvc),
                                  prefix + os.path.basename(self.data.dvc) + suffix)
        return Path(state_file, self._git)

    @cached_property
    def state(self):
        return self._state('', self.STATE_FILE_SUFFIX)

    @cached_property
    def cache_dir_abs(self):
        return os.path.join(self._git.git_dir_abs, ConfigI.CACHE_DIR)

    @cached_property
    def local_state(self):
        return self._state(self.LOCAL_STATE_FILE_PREFIX, self.LOCAL_STATE_FILE_SUFFIX)

    @cached_property
    def cache_dir(self):
        return os.path.join(self._git.git_dir_abs, self._config.cache_dir)

    @property
    def cache(self):
        cache_dir = self.cache_dir_abs

        if self._cache_file:
            file_name = os.path.relpath(os.path.realpath(self._cache_file), cache_dir)
        else:
            file_name = StateFile.load(self).md5

        cache_file = os.path.join(cache_dir, file_name)
        return Path(cache_file, self._git)

    def move_data_to_cache(self):
        md5 = file_md5(self.data.relative)[0]
        self._cache_file = os.path.join(self.cache_dir_abs, md5)
        self._git.modify_gitignore([self.data.relative])
        if not os.path.isfile(self.cache.relative):
            System.hardlink(self.data.relative, self.cache.relative)
        os.chmod(self.data.relative, stat.S_IREAD)

        StateFile(self, md5=md5).save()
        LocalStateFile(self).save()
