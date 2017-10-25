import os
import shutil

from dvc.path.path import Path
from dvc.exceptions import DvcException
from dvc.system import System
from dvc.utils import cached_property
from dvc.data_cloud import file_md5


class DataItemError(DvcException):
    def __init__(self, msg):
        super(DataItemError, self).__init__('Data item error: {}'.format(msg))


class DataDirError(DvcException):
    def __init__(self, msg):
        super(DataDirError, self).__init__(msg)


class DataItemInStatusDirError(DataDirError):
    def __init__(self, file):
        msg = 'File "{}" is in state directory'.format(file)
        super(DataItemInStatusDirError, self).__init__(msg)


class NotInGitDirError(DataDirError):
    def __init__(self, file, git_dir):
        msg = 'File "{}" is not in git directory "{}"'.format(file, git_dir)
        super(NotInGitDirError, self).__init__(msg)


class DataItem(object):
    STATE_FILE_SUFFIX = '.state'
    CACHE_FILE_SEP = '_'

    def __init__(self, data_file, git, config, cache_file=None):
        self._git = git
        self._config = config
        self._cache_file = cache_file

        self._data = Path(data_file, git)

        if not self._data.abs.startswith(self._git.git_dir_abs):
            raise NotInGitDirError(data_file, self._git.git_dir_abs)

        if self._data.abs.startswith(self.state_dir_abs):
            raise DataItemInStatusDirError(data_file)

        pass

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

    @cached_property
    def state(self):
        state_dir = os.path.join(self._git.git_dir_abs, self._config.state_dir)
        state_file = os.path.join(state_dir, self.data.dvc + self.STATE_FILE_SUFFIX)
        return Path(state_file, self._git)

    @cached_property
    def cache_dir(self):
        return os.path.join(self._git.git_dir_abs, self._config.cache_dir)

    @cached_property
    def cache(self):
        cache_dir = self.cache_dir

        if self._cache_file:
            file_name = os.path.relpath(os.path.realpath(self._cache_file), cache_dir)
        else:
            from dvc.state_file import StateFile
            file_name = StateFile.load(self, self._git).md5

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
    def state_dir_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.state_dir)

    def move_data_to_cache(self):
        md5 = file_md5(self.data.relative)[0]
        self._cache_file = os.path.join(self.cache_dir, md5)
        self._git.modify_gitignore([self.data.relative])
        if os.path.isfile(self.cache.relative):
            return
        System.hardlink(self.data.relative, self.cache.relative)
