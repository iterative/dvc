import os

from dvc.command.common.base import CmdBase
from dvc.config import ConfigI
from dvc.logger import Logger
from dvc.state_file import StateFile
from dvc.path.data_item import DataItem
from dvc.system import System
from dvc.command.checkout import CmdCheckout



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
            if fname.startswith(self.settings.config.state_dir) and fname.endswith(DataItem.CACHE_STATE_FILE_SUFFIX):
                data = os.path.relpath(fname, self.settings.config.state_dir)[:-len(DataItem.CACHE_STATE_FILE_SUFFIX)]
                dlist.append(data)
        return dlist

    def collect_targets(self):
        targets = []
        flist = self.collect_data()
        items = self.settings.path_factory.to_data_items(flist)[0]

        for item in items:
            try:
                state = StateFile.load(item, self.settings)
            except Exception as ex:
                Logger.error('Failed to load state file for {}'.format(item.data.relative), exc_info=True)
                return None

            if not state.argv and state.locked:
                targets.append(item)

        return targets

    def checkout_targets(self, targets):
        data = []
        for item in targets:
            self.git.checkout_file_before_last_merge(item.state.relative)
            self.git.checkout_file_before_last_merge(item.cache_state.relative)

            CmdCheckout.checkout([item])

        msg = 'DVC merge files: {}'.format(' '.join(data))
        self.commit_if_needed(msg)

    def run(self):
        targets = self.collect_targets()
        if not targets:
            return 1

        self.checkout_targets(targets)
        self.print_info(targets)

        return 0
