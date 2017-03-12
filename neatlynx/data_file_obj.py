import os

from neatlynx.exceptions import NeatLynxException


class DataFilePathError(NeatLynxException):
    def __init__(self, msg):
        NeatLynxException.__init__(self, 'Data file error: {}'.format(msg))


class NotInDataDirError(NeatLynxException):
    def __init__(self, file, data_dir):
        NeatLynxException.__init__(self,
                                   'Data file location error: the file "{}" has to be in the data directory "{}"'.
                                   format(file, data_dir))


class DataFileObj(object):
    STATE_FILE_SUFFIX = '.state'
    CACHE_FILE_SEP = '_'

    def __init__(self, data_file, git, config):
        '''
        nlx file name - system file name without data/cache/state prefix
        '''
        self._git = git
        self._config = config
        self._curr_dir_abs = os.path.realpath(os.curdir)

        self._data_file = data_file

        if not self.data_file_abs.startswith(self.data_dir_abs):
            raise NotInDataDirError(self._data_file, self._config.data_dir)
        pass

    def create_symlink(self):
        data_dir = os.path.dirname(self.data_file_relative)
        cache_relative_to_data_dir = os.path.relpath(self.cache_file_relative, data_dir)
        os.symlink(cache_relative_to_data_dir, self.data_file_relative)

    @property
    def data_dir_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.data_dir)

    # Data file properties
    @property
    def data_file_name(self):
        return os.path.basename(self._data_file)

    @property
    def data_file_nlx(self):
        return os.path.relpath(self.data_file_abs, self.data_dir_abs)
    
    # @property
    # def data_file_abs(self):
    #     return os.path.realpath(self.data_file_relative)

    @property
    def data_file_abs(self):
        '''Do not fully resolve file name since it is a link'''
        return os.path.abspath(self.data_file_relative)

    @property
    def data_file_relative(self):
        return self._data_file

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
        return self.data_file_nlx + self.CACHE_FILE_SEP + self._git.curr_commit

    @property
    def cache_file_aws_key(self):
        return '{}/{}'.format(self._config.aws_storage_prefix, self.cache_file_nlx).strip('/')

    @property
    def cache_file_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.cache_dir, self.cache_file_nlx)

    @property
    def cache_file_relative(self):
        return os.path.relpath(self.cache_file_abs, self._curr_dir_abs)

    # State file properties
    @property
    def state_file_name(self):
        return self.data_file_name + self.STATE_FILE_SUFFIX

    @property
    def state_file_nlx(self):
        return self.data_file_nlx + self.STATE_FILE_SUFFIX

    @property
    def state_file_abs(self):
        return os.path.join(self._git.git_dir_abs, self._config.state_dir, self.state_file_nlx)

    @property
    def state_file_relative(self):
        return os.path.relpath(self.state_file_abs, self._curr_dir_abs)


class DataFileObjExisting(DataFileObj):
    def __init__(self, data_file, git, config):
        DataFileObj.__init__(self, data_file, git, config)

        if not os.path.islink(data_file):
            raise DataFilePathError('Data file must be a symbolic link')
        pass

    @property
    def cache_file_name(self):
        return os.path.basename(self.cache_file_abs)

    @property
    def cache_file_abs(self):
        return os.path.realpath(self.data_file_relative)

    @staticmethod
    def remove_dir_if_empty(dir):
        cache_file_dir = os.path.dirname(dir)
        if cache_file_dir != '' and not os.listdir(cache_file_dir):
            os.rmdir(cache_file_dir)

    def remove_state_dir_if_empty(self):
        self.remove_dir_if_empty(self.state_file_relative)

    def remove_cache_dir_if_empty(self):
        self.remove_dir_if_empty(self.cache_file_relative)
