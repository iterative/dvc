import os

from dvc.path.data_item import DataItem, DataItemError, DataDirError
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

    def is_data_item(self, fname):
        data = os.path.relpath(os.path.realpath(fname), self._git.git_dir_abs)
        state = os.path.join(self._config.state_dir, data + DataItem.STATE_FILE_SUFFIX)
        return os.path.isfile(state)

    def existing_data_item(self, fname):
        if not self.is_data_item(fname):
            raise DataItemError(u'No state file found for data file {}'.format(fname))

        return DataItem(fname, self._git, self._config)

    def to_data_items(self, files):
        result = []
        externally_created_files = []

        for file in files:
            try:
                data_item = DataItem(file, self._git, self._config)
                result.append(data_item)
            except DataDirError:
                externally_created_files.append(file)

        return result, externally_created_files

    def all_existing_data_items(self):
        items = []
        states = []

        for root, dirs, files in os.walk(self._config.state_dir):
            for fname in files:
                path = os.path.join(root, fname)

                if not fname.endswith(DataItem.STATE_FILE_SUFFIX):
                    Logger.warn('Found file \'{}\' without state suffix inside state dir'.format(path))
                    continue

                states.append(path)

        for state in states:
            data = os.path.relpath(state, self._config.state_dir)[:-len(DataItem.STATE_FILE_SUFFIX)]
            items.append(self.existing_data_item_from_dvc_path(data))

        return items
