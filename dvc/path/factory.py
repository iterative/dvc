import os
import re

from dvc.config import ConfigI
from dvc.path.data_item import DataItem, DataItemError, DataDirError
from dvc.path.path import Path
from dvc.path.stated_data_item import StatedDataItem
from dvc.system import System
from dvc.state_file import StateFile


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
        item = DataItem(fname, self._git, self._config)
        return StateFile.find(item) != None

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

    def all_existing_data_items(self, subdir='.', cache_exists=True):
        files = StateFile.find_all_data_files(self._git, subdir)
        return self.to_data_items(files)[0]
