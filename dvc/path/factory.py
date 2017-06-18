import os

from dvc.path.data_item import DataItem, DataItemError, NotInDataDirError
from dvc.path.path import Path
from dvc.path.stated_data_item import StatedDataItem
from dvc.system import System


class PathFactory(object):
    def __init__(self, git, config):
        self._git = git
        self._config = config
        self._curr_dir_abs = System.realpath(os.curdir)

    def path(self, relative_raw):
        return Path(relative_raw, self._git)

    def data_item(self, data_file, cache_file=None):
        return DataItem(data_file, self._git, self._config, cache_file)

    def stated_data_item(self, state, data_file):
        return StatedDataItem(state, data_file, self._git, self._config)

    def existing_data_item_from_dvc_path(self, dvc_path):
        path = Path.from_dvc_path(dvc_path, self._git)
        return self.existing_data_item(path.relative)

    def existing_data_item(self, file):
        if not System.islink(file):
            raise DataItemError(u'Data file "%s" must be a symbolic link' % file)
        resolved_symlink = System.realpath(file)
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
