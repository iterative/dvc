import os
import json

from dvc.command.base import CmdBase, DvcLock
from dvc.logger import Logger
from dvc.state_file import StateFile
from dvc.path.data_item import DataItem
from dvc.system import System

class CmdMerge(CmdBase):
    def __init__(self, settings):
        super(CmdMerge, self).__init__(settings)

    def print_info(self, targets):
        for item in targets:
            Logger.info('Restored original data after merge:')
            Logger.info(' {}'.format(item.data.relative))

    def collect_data(self):
        dlist = []
        flist = self.git.get_last_merge_changed_files()
        for fname in flist:
            if fname.startswith(self.settings.config.state_dir) and fname.endswith(DataItem.STATE_FILE_SUFFIX):
                data = os.path.relpath(fname, self.settings.config.state_dir)[:-len(DataItem.STATE_FILE_SUFFIX)]
                dlist.append(data)
        return dlist

    def collect_targets(self):
        targets = []
        flist = self.collect_data()
        items = self.settings.path_factory.to_data_items(flist)[0]

        for item in items:
            try:
                state = StateFile.load(item, self.git)
            except Exception as ex:
                Logger.error('Failed to load state file for {}'.format(item.data.relative), exc_info=True)
                return None

            if not state.command == StateFile.COMMAND_IMPORT_FILE:
                continue

            targets.append(item)

        return targets

    def checkout_targets(self, targets):
        data = []
        for item in targets:
            self.git.checkout_file_before_last_merge(item.state.relative)

            # Reload data item, so we can get restored cache
            new_item = self.settings.path_factory.data_item(item.data.relative)
            if os.path.isfile(new_item.data.relative):
                os.remove(new_item.data.relative)

            System.hardlink(new_item.cache.relative, new_item.data.relative)

            data.append(item.data.relative)

        msg = 'DVC merge files: {}'.format(' '.join(data))
        self.commit_if_needed(msg)

    def run(self):
        with DvcLock(self.is_locker, self.git):
            targets = self.collect_targets()
            if not targets:
                return 1

            self.checkout_targets(targets)
            self.print_info(targets)

            return 0
