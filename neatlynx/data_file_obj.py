import os

from neatlynx.exceptions import NeatLynxException


class DataFilePathError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Data file error: {}'.format(msg))


class NotInDataDirError(NeatLynxException):
    def __init__(self, data_dir):
        NeatLynxException.__init__(self,
                                   'Data file location error: the file has to be in the data directory "{}"'.
                                   format(data_dir))


class DataFileObj(object):
    STATE_FILE_SUFFIX = '.state'
    CACHE_FILE_SEP = '_'

    def __init__(self, data_file, git, config):
        self._git = git
        self._config = config
        self._curr_dir_abs = os.path.realpath(os.curdir)

        data_file = os.path.realpath(data_file)

        self._data_file_name = os.path.basename(data_file)
        data_file_dir = os.path.realpath(os.path.dirname(data_file))

        data_dir = os.path.join(self._git.git_dir_abs, config.data_dir)
        self._rel_path = os.path.relpath(data_file_dir, data_dir)

        if not data_file_dir.startswith(data_dir):
            raise NotInDataDirError(config.data_dir)

        if self._rel_path == '.':
            self._rel_path = ''
        pass

    # Data file properties
    @property
    def data_file_name(self):
        return self._data_file_name

    @property
    def data_file_nlx(self):
        return os.path.join(self._config.data_dir, self._rel_path, self.data_file_name)
    
    @property
    def data_file_abs(self):
        return os.path.join(self._git.git_dir_abs, self.data_file_nlx)

    @property
    def data_file_relative(self):
        return os.path.relpath(self.data_file_abs, self._curr_dir_abs)

    # Cache file properties
    @property
    def cache_file_name(self):
        if not self._git.curr_commit:
            return None
        return self.data_file_name + self.CACHE_FILE_SEP + self._git.curr_commit

    @property
    def cache_file_nlx(self):
        if not self._git.curr_commit:
            return None
        return os.path.join(self._config.cache_dir, self._rel_path, self.cache_file_name)

    @property
    def cache_file_abs(self):
        return os.path.join(self._git.git_dir_abs, self.cache_file_nlx)

    @property
    def cache_file_relative(self):
        return os.path.relpath(self.cache_file_abs, self._curr_dir_abs)

    # State file properties
    @property
    def state_file_name(self):
        return self.data_file_name + self.STATE_FILE_SUFFIX

    @property
    def state_file_nlx(self):
        return os.path.join(self._config.state_dir, self._rel_path, self.state_file_name)

    @property
    def state_file_abs(self):
        return os.path.join(self._git.git_dir_abs, self.state_file_nlx)

    @property
    def state_file_relative(self):
        return os.path.relpath(self.state_file_abs, self._curr_dir_abs)
