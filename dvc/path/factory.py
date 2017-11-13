import os

from dvc.config import ConfigI
from dvc.path.data_item import DataItem, DataItemError, DataDirError
from dvc.path.path import Path
from dvc.path.stated_data_item import StatedDataItem
from dvc.system import System
from dvc.logger import Logger

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

    def data_item_from_dvc_path(self, dvc_path, existing=True):
        path = Path.from_dvc_path(dvc_path, self._git)
        if existing:
            return self.existing_data_item(path.relative)
        else:
            return self.data_item(path.relative)

    def is_data_item(self, fname):
        data = os.path.relpath(os.path.realpath(fname), self._git.git_dir_abs)
        state = os.path.join(ConfigI.STATE_DIR, data + DataItem.STATE_FILE_SUFFIX)
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

    def all_existing_data_items(self, subdir='.'):
        states = []

        for root, dirs, files in os.walk(os.path.join(ConfigI.STATE_DIR, subdir)):
            for fname in files:
                path = os.path.join(root, fname)

                if not fname.endswith(DataItem.STATE_FILE_SUFFIX):
                    Logger.warn('Found file \'{}\' without state suffix inside state dir'.format(path))
                    continue

                states.append(path)

        return self.data_items_from_states(states)

    def data_items_from_states(self, states, existing=True):
        return map(lambda s: self.data_item_from_dvc_path(self.state_path_to_dvc_path(s), existing),
                   states)

    @staticmethod
    def state_path_to_dvc_path(state):
        return os.path.relpath(state, ConfigI.STATE_DIR)[:-len(DataItem.STATE_FILE_SUFFIX)]
