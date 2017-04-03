import os

from dvc.path.data_item import DataItem, DataItemError, NotInDataDirError
from dvc.path.path import Path
from dvc.path.stated_data_item import StatedDataItem


class PathFactory(object):
    def __init__(self, git, config):
        self._git = git
        self._config = config
        self._curr_dir_abs = os.path.realpath(os.curdir)

    def path(self, relative_raw):
        return Path(relative_raw, self._git)

    def data_item(self, data_file):
        return DataItem(data_file, self._git, self._config)

    def stated_data_item(self, state, data_file):
        return StatedDataItem(state, data_file, self._git, self._config)

    def existing_data_item(self, file):
        if not os.path.islink(file):
            raise DataItemError(u'Data file "%s" must be a symbolic link' % file)
        resolved_symlink = os.path.realpath(file)
        return DataItem(file, self._git, self._config, resolved_symlink)

    def to_data_items(self, files):
        result = []
        externally_created_files = []

        for file in files:
            try:
                data_item = DataItem(file, self._git, self._config)
                result.append(data_item)
            except NotInDataDirError:
                externally_created_files.append(file)

        return result, externally_created_files
