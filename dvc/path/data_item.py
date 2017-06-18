import os
import re
import shutil
import fnmatch

from dvc.path.path import Path
from dvc.exceptions import DvcException
from dvc.system import System
from dvc.utils import cached_property


class DataItemError(DvcException):
    def __init__(self, msg):
        super(DataItemError, self).__init__('Data item error: {}'.format(msg))


class NotInDataDirError(DvcException):
    def __init__(self, msg):
        super(NotInDataDirError, self).__init__(msg)

    @staticmethod
    def build(file, data_dir):
        msg = 'the file "{}" is not in the data directory "{}"'.format(file, data_dir)
        return NotInDataDirError(msg)


class DataItemInStatusDirError(NotInDataDirError):
    def __init__(self, msg):
        super(DataItemInStatusDirError, self).__init__(msg)

    @staticmethod
    def build(file, data_dir):
        msg = 'the file "{}" is in state directory, not in the data directory "{}"'.format(
                file, data_dir)
        return DataItemInStatusDirError(msg)


class NotInGitDirError(NotInDataDirError):
    def __init__(self, msg):
        super(NotInGitDirError, self).__init__(msg)

    @staticmethod
    def build(file, git_dir):
        msg = 'the file "{}" is not in git directory "{}"'.format(file, git_dir)
        return NotInGitDirError(msg)


class DataItem(object):
    STATE_FILE_SUFFIX = '.state'
    CACHE_FILE_SEP = '_'

    def __init__(self, data_file, git, config, cache_file=None):
        self._git = git
        self._config = config
        self._cache_file = cache_file

        self._data = Path(data_file, git)

        if not self._data.abs.startswith(self._git.git_dir_abs):
            raise NotInGitDirError.build(data_file, self._git.git_dir_abs)

        if self._data.abs.startswith(self.state_dir_abs):
            raise DataItemInStatusDirError.build(data_file, self._config.data_dir)

        if not self._data.abs.startswith(self.data_dir_abs):
            raise NotInDataDirError.build(data_file, self._config.data_dir)

        if not self._data.dvc.startswith(self._config.data_dir):
            raise NotInDataDirError.build(data_file, self._config.data_dir)
        pass

    def copy(self, cache_file=None):
        if not cache_file:
            cache_file = self._cache_file

        return DataItem(self._data.abs, self._git, self._config, cache_file)

    def __hash__(self):
        return self.data.dvc.__hash__()

    def __eq__(self, other):
        return self.data.dvc == other.data.dvc

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
        return Path(state_file, self._git)

    @cached_property
    def cache(self):
        cache_dir = os.path.join(self._git.git_dir_abs, self._config.cache_dir)

        if self._cache_file:
            file_name = os.path.relpath(os.path.realpath(self._cache_file), cache_dir)
        else:
            file_name = self.data_dvc_short + self.CACHE_FILE_SEP + self._git.curr_commit

        cache_file = os.path.join(cache_dir, file_name)
        return Path(cache_file, self._git)

    def get_all_caches(self):
        result = []

        suffix_len = self._git.COMMIT_LEN + len(self.CACHE_FILE_SEP)
        cache_prefix = os.path.basename(self.resolved_cache.relative[:-suffix_len])

        for cache_file in os.listdir(self.resolved_cache.dirname):
            if cache_file[:-suffix_len] == cache_prefix:
                data_item = self.copy(os.path.join(self.resolved_cache.dirname, cache_file))
                result.append(data_item)

        return result

    @cached_property
    def resolved_cache(self):
        resolved_cache = os.path.realpath(self._data.relative)
        return Path(resolved_cache, self._git)

    @cached_property
    def data_dir_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.data_dir)

    @cached_property
    def state_dir_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.state_dir)

    @property
    def symlink_file(self):
        data_file_dir = os.path.dirname(self.data.relative)
        return os.path.relpath(self.cache.relative, data_file_dir)

    def move_data_to_cache(self):
        cache_dir = os.path.dirname(self.cache.relative)
        if not os.path.isdir(cache_dir):
            os.makedirs(cache_dir)

        shutil.move(self.data.relative, self.cache.relative)
        System.symlink(self.symlink_file, self.data.relative)
