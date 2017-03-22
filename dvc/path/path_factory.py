import os

from dvc.path.data_path import DataPath, DataFilePathError, NotInDataDirError
from dvc.path.dvc_path import DvcPath


class PathFactory(object):
    def __init__(self, git, config):
        self._git = git
        self._config = config
        self._curr_dir_abs = os.path.realpath(os.curdir)

    def path(self, relative_raw):
        return DvcPath(relative_raw, self._git)

    def data_path(self, data_file):
        return DataPath(data_file, self._git, self._config)

    def existing_data_path(self, data_file):
        if not os.path.islink(data_file):
            raise DataFilePathError(u'Data file "%s" must be a symbolic link' % data_file)
        resolved_symlink = os.path.realpath(data_file)
        return DataPath(data_file, self._git, self._config, resolved_symlink)

    def to_data_path(self, files):
        result = []
        externally_created_files = []

        for file in files:
            try:
                data_path = DataPath(file, self._git, self._config)
                result.append(data_path)
            except NotInDataDirError:
                externally_created_files.append(file)

        return result, externally_created_files
